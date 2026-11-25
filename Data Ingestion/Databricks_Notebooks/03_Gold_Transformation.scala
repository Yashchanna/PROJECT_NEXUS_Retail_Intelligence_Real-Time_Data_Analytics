// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

val storageAccount = "nexusretail2026"

val silverBase =
  s"abfss://curated@$storageAccount.dfs.core.windows.net"

val goldBase =
  s"abfss://analytics@$storageAccount.dfs.core.windows.net"

println("===== PROJECT NEXUS GOLD TRANSFORMATION =====")

val silverCustomers = spark.read
  .format("delta")
  .load(s"$silverBase/customers")

val silverProducts = spark.read
  .format("delta")
  .load(s"$silverBase/products")

val silverOrders = spark.read
  .format("delta")
  .load(s"$silverBase/orders")

println("===== SILVER INPUT VALIDATION =====")

println(s"Customers: ${silverCustomers.count()}")
println(s"Products: ${silverProducts.count()}")
println(s"Orders: ${silverOrders.count()}")

println("===== SILVER SCHEMAS =====")

silverCustomers.printSchema()
silverProducts.printSchema()
silverOrders.printSchema()

assert(silverCustomers.count() > 0, "Customers table is empty")
assert(silverProducts.count() > 0, "Products table is empty")
assert(silverOrders.count() > 0, "Orders table is empty")

println("GOLD INPUT VALIDATION PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING GOLD DIMENSIONS =====")

// Customer Dimension
val dimCustomer = silverCustomers
  .select(
    col("customer_id"),
    col("customer_name"),
    col("email"),
    col("city"),
    col("signup_date")
  )
  .dropDuplicates("customer_id")

// Product Dimension
val dimProduct = silverProducts
  .select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("unit_price"),
    col("active")
  )
  .dropDuplicates("product_id")

println("===== DIMENSION VALIDATION =====")

val customerCount = dimCustomer.count()
val productCount = dimProduct.count()

println(s"dim_customer: $customerCount")
println(s"dim_product: $productCount")

assert(
  customerCount == silverCustomers.count(),
  "Customer dimension count mismatch"
)

assert(
  productCount == silverProducts.count(),
  "Product dimension count mismatch"
)

assert(
  dimCustomer.groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count() == 0,
  "Duplicate customer IDs found"
)

assert(
  dimProduct.groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count() == 0,
  "Duplicate product IDs found"
)

println("DIMENSION VALIDATION PASSED")

println("===== SAVING GOLD DIMENSIONS =====")

dimCustomer.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/dim_customer")

dimProduct.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/dim_product")

println("===== VERIFYING SAVED DIMENSIONS =====")

val savedCustomerCount = spark.read
  .format("delta")
  .load(s"$goldBase/dim_customer")
  .count()

val savedProductCount = spark.read
  .format("delta")
  .load(s"$goldBase/dim_product")
  .count()

assert(
  savedCustomerCount == customerCount,
  "Saved customer dimension mismatch"
)

assert(
  savedProductCount == productCount,
  "Saved product dimension mismatch"
)

println(s"dim_customer saved: $savedCustomerCount rows")
println(s"dim_product saved: $savedProductCount rows")

println("GOLD CUSTOMER AND PRODUCT DIMENSIONS COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING DATE DIMENSION =====")

val dateRange = silverOrders
  .agg(
    min("order_date").alias("min_date"),
    max("order_date").alias("max_date")
  )
  .first()

val minDate = dateRange.getDate(0)
val maxDate = dateRange.getDate(1)

println(s"Minimum order date: $minDate")
println(s"Maximum order date: $maxDate")

val dimDate = spark.sql(
  s"""
    SELECT explode(
      sequence(
        DATE('$minDate'),
        DATE('$maxDate'),
        INTERVAL 1 DAY
      )
    ) AS full_date
  """
)
  .withColumn(
    "date_key",
    date_format(col("full_date"), "yyyyMMdd").cast("int")
  )
  .withColumn("year", year(col("full_date")))
  .withColumn("quarter", quarter(col("full_date")))
  .withColumn("month", month(col("full_date")))
  .withColumn("day", dayofmonth(col("full_date")))
  .withColumn("month_name", date_format(col("full_date"), "MMMM"))
  .withColumn("day_of_week", dayofweek(col("full_date")))
  .withColumn("week_of_year", weekofyear(col("full_date")))
  .withColumn(
    "is_weekend",
    dayofweek(col("full_date")).isin(1, 7)
  )
  .select(
    "date_key",
    "full_date",
    "year",
    "quarter",
    "month",
    "day",
    "month_name",
    "day_of_week",
    "week_of_year",
    "is_weekend"
  )

println("===== DATE DIMENSION VALIDATION =====")

val expectedDateCount =
  java.time.temporal.ChronoUnit.DAYS.between(
    minDate.toLocalDate,
    maxDate.toLocalDate
  ) + 1L

val actualDateCount = dimDate.count()

println(s"Expected calendar dates: $expectedDateCount")
println(s"Generated calendar dates: $actualDateCount")

assert(
  actualDateCount == expectedDateCount,
  "Date dimension count mismatch"
)

assert(
  dimDate.select("date_key").distinct().count() == actualDateCount,
  "Duplicate date keys found"
)

println("DATE DIMENSION VALIDATION PASSED")

println("===== SAVING DATE DIMENSION =====")

dimDate.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/dim_date")

val savedDateCount = spark.read
  .format("delta")
  .load(s"$goldBase/dim_date")
  .count()

assert(
  savedDateCount == actualDateCount,
  "Saved date dimension count mismatch"
)

println(s"dim_date saved: $savedDateCount rows")

println("===== SAMPLE DATE DIMENSION =====")

dimDate
  .orderBy("full_date")
  .show(5, false)

println("GOLD DATE DIMENSION COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING FACT SALES =====")

val factSales = silverOrders.alias("o")
  .join(
    dimCustomer.alias("c"),
    col("o.customer_id") === col("c.customer_id"),
    "inner"
  )
  .join(
    dimProduct.alias("p"),
    col("o.product_id") === col("p.product_id"),
    "inner"
  )
  .join(
    dimDate.alias("d"),
    col("o.order_date") === col("d.full_date"),
    "inner"
  )
  .select(
    col("o.order_id"),
    col("o.customer_id"),
    col("o.product_id"),
    col("d.date_key"),
    col("o.order_date"),
    col("o.quantity"),
    col("o.order_status"),
    col("o.last_updated"),
    col("p.unit_price"),
    (
      col("o.quantity").cast("decimal(18,2)") *
      col("p.unit_price")
    ).cast("decimal(20,2)").alias("gross_order_amount"),
    when(
      col("o.order_status") === "DELIVERED",
      (
        col("o.quantity").cast("decimal(18,2)") *
        col("p.unit_price")
      ).cast("decimal(20,2)")
    ).otherwise(lit(0).cast("decimal(20,2)"))
      .alias("revenue")
  )

println("===== FACT SALES VALIDATION =====")

val silverOrderCount = silverOrders.count()
val factSalesCount = factSales.count()

println(s"Silver orders: $silverOrderCount")
println(s"Fact sales: $factSalesCount")

assert(
  silverOrderCount == factSalesCount,
  "Fact sales row count mismatch"
)

assert(
  factSales.select("order_id").distinct().count() == factSalesCount,
  "Duplicate order IDs in fact sales"
)

assert(
  factSales.filter(
    col("revenue").isNull || col("revenue") < 0
  ).count() == 0,
  "Invalid revenue found"
)

assert(
  factSales.filter(
    col("order_status") =!= "DELIVERED" &&
    col("revenue") =!= 0
  ).count() == 0,
  "Non-delivered orders have recognized revenue"
)

println("FACT SALES VALIDATION PASSED")

println("===== SAVING FACT SALES =====")

factSales.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/fact_sales")

val savedFactCount = spark.read
  .format("delta")
  .load(s"$goldBase/fact_sales")
  .count()

assert(
  savedFactCount == factSalesCount,
  "Saved fact sales count mismatch"
)

println(s"fact_sales saved: $savedFactCount rows")

println("===== SALES SUMMARY =====")

factSales
  .groupBy("order_status")
  .agg(
    count("*").alias("orders"),
    sum("revenue").alias("recognized_revenue")
  )
  .orderBy("order_status")
  .show(false)

println("===== TOTAL RECOGNIZED REVENUE =====")

factSales
  .agg(
    sum("revenue").alias("total_revenue"),
    sum(
      when(col("order_status") === "DELIVERED", col("quantity"))
        .otherwise(lit(0))
    ).alias("delivered_units")
  )
  .show(false)

println("GOLD FACT SALES COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

val savedFactSales = spark.read
  .format("delta")
  .load(s"$goldBase/fact_sales")

println("===== PERSISTED FACT SALES VALIDATION =====")

println(s"Saved rows: ${savedFactSales.count()}")

savedFactSales
  .groupBy("order_status")
  .agg(
    count("*").alias("orders"),
    sum("revenue").alias("recognized_revenue")
  )
  .orderBy("order_status")
  .show(false)

println("===== REVENUE VERIFICATION =====")

savedFactSales
  .agg(
    sum("revenue").alias("total_revenue"),
    sum(
      when(col("order_status") === "DELIVERED", col("quantity"))
        .otherwise(lit(0))
    ).alias("delivered_units")
  )
  .show(false)

assert(savedFactSales.count() == 90, "Unexpected fact row count")

println("PERSISTED FACT SALES VERIFIED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING SALES BY CITY =====")

val salesByCity = savedFactSales.alias("f")
  .join(
    dimCustomer.alias("c"),
    col("f.customer_id") === col("c.customer_id"),
    "inner"
  )
  .groupBy(col("c.city"))
  .agg(
    count("*").alias("total_orders"),

    sum(
      when(col("f.order_status") === "DELIVERED", 1)
        .otherwise(0)
    ).alias("delivered_orders"),

    sum(
      when(col("f.order_status") === "DELIVERED", col("f.quantity"))
        .otherwise(0)
    ).alias("units_sold"),

    sum(col("f.revenue"))
      .cast("decimal(20,2)")
      .alias("total_revenue"),

    countDistinct(col("f.customer_id"))
      .alias("unique_customers")
  )
  .orderBy(desc("total_revenue"))

println("===== CITY SALES RESULTS =====")

salesByCity.show(false)

println("===== VALIDATION =====")

val cityRevenue = salesByCity
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val factRevenue = savedFactSales
  .agg(sum("revenue"))
  .first()
  .getDecimal(0)

assert(
  cityRevenue.compareTo(factRevenue) == 0,
  "City revenue reconciliation failed"
)

println("CITY REVENUE RECONCILIATION PASSED")

println("===== SAVING SALES BY CITY =====")

salesByCity.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/sales_by_city")

val savedSalesByCity = spark.read
  .format("delta")
  .load(s"$goldBase/sales_by_city")

assert(
  savedSalesByCity.count() == salesByCity.count(),
  "Saved city analytics count mismatch"
)

println(s"Cities saved: ${savedSalesByCity.count()}")

println("SALES BY CITY COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING SALES BY PRODUCT =====")

val salesByProduct = savedFactSales.alias("f")
  .join(
    dimProduct.alias("p"),
    col("f.product_id") === col("p.product_id"),
    "inner"
  )
  .groupBy(
    col("p.product_id"),
    col("p.product_name"),
    col("p.category")
  )
  .agg(
    count("*").alias("total_orders"),

    sum(
      when(col("f.order_status") === "DELIVERED", 1)
        .otherwise(0)
    ).alias("delivered_orders"),

    sum(
      when(col("f.order_status") === "DELIVERED", col("f.quantity"))
        .otherwise(0)
    ).alias("units_sold"),

    sum(col("f.revenue"))
      .cast("decimal(20,2)")
      .alias("total_revenue")
  )

println("===== TOP PRODUCTS BY REVENUE =====")

salesByProduct
  .orderBy(desc("total_revenue"))
  .show(10, false)

println("===== PRODUCT REVENUE VALIDATION =====")

val productRevenue = salesByProduct
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val factRevenueForProducts = savedFactSales
  .agg(sum("revenue"))
  .first()
  .getDecimal(0)

assert(
  productRevenue.compareTo(factRevenueForProducts) == 0,
  "Product revenue reconciliation failed"
)

assert(
  salesByProduct
    .agg(sum("total_orders"))
    .first()
    .getLong(0) == savedFactSales.count(),
  "Product order count reconciliation failed"
)

println("PRODUCT RECONCILIATION PASSED")

println("===== SAVING SALES BY PRODUCT =====")

salesByProduct.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/sales_by_product")

val savedSalesByProduct = spark.read
  .format("delta")
  .load(s"$goldBase/sales_by_product")

assert(
  savedSalesByProduct.count() == salesByProduct.count(),
  "Saved product analytics count mismatch"
)

println(s"Products saved: ${savedSalesByProduct.count()}")

println("SALES BY PRODUCT COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== VERIFYING SALES BY PRODUCT =====")

val verifiedProductSales = spark.read
  .format("delta")
  .load(s"$goldBase/sales_by_product")

val productCount = verifiedProductSales.count()

val totalOrders = verifiedProductSales
  .agg(sum("total_orders"))
  .first()
  .getLong(0)

val totalRevenue = verifiedProductSales
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

println(s"Products saved: $productCount")
println(s"Orders reconciled: $totalOrders")
println(s"Revenue reconciled: $totalRevenue")

assert(totalOrders == 90, "Order count mismatch")

assert(
  totalRevenue.compareTo(new java.math.BigDecimal("253287.00")) == 0,
  "Revenue mismatch"
)

println("SALES BY PRODUCT VERIFIED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING MONTHLY SALES =====")

val monthlySales = savedFactSales
  .groupBy(
    year(col("order_date")).alias("year"),
    month(col("order_date")).alias("month")
  )
  .agg(
    count("*").alias("total_orders"),

    sum(
      when(col("order_status") === "DELIVERED", 1)
        .otherwise(0)
    ).alias("delivered_orders"),

    sum(
      when(col("order_status") === "DELIVERED", col("quantity"))
        .otherwise(0)
    ).alias("units_sold"),

    sum(col("revenue"))
      .cast("decimal(20,2)")
      .alias("total_revenue")
  )
  .withColumn(
    "year_month",
    format_string("%04d-%02d", col("year"), col("month"))
  )
  .select(
    "year_month",
    "year",
    "month",
    "total_orders",
    "delivered_orders",
    "units_sold",
    "total_revenue"
  )
  .orderBy("year_month")

println("===== MONTHLY SALES RESULTS =====")

monthlySales.show(false)

println("===== MONTHLY RECONCILIATION =====")

val monthlyOrderCount = monthlySales
  .agg(sum("total_orders"))
  .first()
  .getLong(0)

val monthlyRevenue = monthlySales
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val factOrderCount = savedFactSales.count()

val factRevenue = savedFactSales
  .agg(sum("revenue"))
  .first()
  .getDecimal(0)

assert(
  monthlyOrderCount == factOrderCount,
  "Monthly order count mismatch"
)

assert(
  monthlyRevenue.compareTo(factRevenue) == 0,
  "Monthly revenue mismatch"
)

println("MONTHLY RECONCILIATION PASSED")

println("===== SAVING MONTHLY SALES =====")

monthlySales.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/monthly_sales")

val savedMonthlySales = spark.read
  .format("delta")
  .load(s"$goldBase/monthly_sales")

assert(
  savedMonthlySales.count() == monthlySales.count(),
  "Saved monthly sales count mismatch"
)

println(s"Months saved: ${savedMonthlySales.count()}")

println("MONTHLY SALES COMPLETED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== CREATING SALES KPIS =====")

val salesKpis = savedFactSales
  .agg(
    count("*").alias("total_orders"),

    sum(
      when(col("order_status") === "DELIVERED", 1)
        .otherwise(0)
    ).alias("delivered_orders"),

    sum(
      when(col("order_status") === "CANCELLED", 1)
        .otherwise(0)
    ).alias("cancelled_orders"),

    sum(
      when(col("order_status") === "RETURNED", 1)
        .otherwise(0)
    ).alias("returned_orders"),

    countDistinct("customer_id").alias("active_customers"),

    countDistinct("product_id").alias("products_ordered"),

    sum(
      when(col("order_status") === "DELIVERED", col("quantity"))
        .otherwise(0)
    ).alias("units_sold"),

    sum("revenue")
      .cast("decimal(20,2)")
      .alias("total_revenue")
  )
  .withColumn(
    "average_order_value",
    when(
      col("delivered_orders") > 0,
      (col("total_revenue") / col("delivered_orders"))
        .cast("decimal(20,2)")
    ).otherwise(lit(0).cast("decimal(20,2)"))
  )
  .withColumn(
    "delivery_rate",
    when(
      col("total_orders") > 0,
      (
        col("delivered_orders").cast("double") /
        col("total_orders") * 100
      ).cast("decimal(10,2)")
    ).otherwise(lit(0).cast("decimal(10,2)"))
  )

println("===== KPI RESULTS =====")

salesKpis.show(false)

println("===== KPI VALIDATION =====")

val kpiRow = salesKpis.first()

assert(
  kpiRow.getAs[Long]("total_orders") == 90,
  "Total orders mismatch"
)

assert(
  kpiRow.getAs[Long]("delivered_orders") == 51,
  "Delivered orders mismatch"
)

assert(
  kpiRow.getAs[java.math.BigDecimal]("total_revenue")
    .compareTo(new java.math.BigDecimal("253287.00")) == 0,
  "Total revenue mismatch"
)

assert(
  salesKpis.count() == 1,
  "KPI table must contain exactly one row"
)

println("KPI VALIDATION PASSED")

println("===== SAVING SALES KPIS =====")

salesKpis.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(s"$goldBase/sales_kpis")

val savedSalesKpis = spark.read
  .format("delta")
  .load(s"$goldBase/sales_kpis")

assert(
  savedSalesKpis.count() == 1,
  "Saved KPI table must contain one row"
)

println("SALES KPIS SAVED SUCCESSFULLY")
println("GOLD ANALYTICS TABLES COMPLETED")