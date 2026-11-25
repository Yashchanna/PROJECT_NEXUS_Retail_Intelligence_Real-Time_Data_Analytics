// Databricks notebook source
import org.apache.spark.sql.functions._

val storageAccount = "nexusretail2026"

val bronzeBase =
  s"abfss://bronze@$storageAccount.dfs.core.windows.net"

val silverBase =
  s"abfss://curated@$storageAccount.dfs.core.windows.net"

val goldBase =
  s"abfss://analytics@$storageAccount.dfs.core.windows.net"

val rejectedBase =
  s"abfss://rejected@$storageAccount.dfs.core.windows.net"

println("===== LOADING BRONZE TABLES =====")

val bronzeCustomers = spark.read.format("delta")
  .load(s"$bronzeBase/customers")

val bronzeProducts = spark.read.format("delta")
  .load(s"$bronzeBase/products")

val bronzeOrders = spark.read.format("delta")
  .load(s"$bronzeBase/orders")

println("===== LOADING SILVER TABLES =====")

val silverCustomers = spark.read.format("delta")
  .load(s"$silverBase/customers")

val silverProducts = spark.read.format("delta")
  .load(s"$silverBase/products")

val silverOrders = spark.read.format("delta")
  .load(s"$silverBase/orders")

println("===== LOADING GOLD TABLES =====")

val dimCustomer = spark.read.format("delta")
  .load(s"$goldBase/dim_customer")

val dimProduct = spark.read.format("delta")
  .load(s"$goldBase/dim_product")

val dimDate = spark.read.format("delta")
  .load(s"$goldBase/dim_date")

val factSales = spark.read.format("delta")
  .load(s"$goldBase/fact_sales")

val salesByCity = spark.read.format("delta")
  .load(s"$goldBase/sales_by_city")

val salesByProduct = spark.read.format("delta")
  .load(s"$goldBase/sales_by_product")

val monthlySales = spark.read.format("delta")
  .load(s"$goldBase/monthly_sales")

val salesKpis = spark.read.format("delta")
  .load(s"$goldBase/sales_kpis")

println("===== DATA QUALITY INPUT COUNTS =====")

println(s"Bronze Customers: ${bronzeCustomers.count()}")
println(s"Bronze Products:  ${bronzeProducts.count()}")
println(s"Bronze Orders:    ${bronzeOrders.count()}")

println(s"Silver Customers: ${silverCustomers.count()}")
println(s"Silver Products:  ${silverProducts.count()}")
println(s"Silver Orders:    ${silverOrders.count()}")

println(s"Gold Fact Sales:  ${factSales.count()}")
println(s"Gold KPI Rows:    ${salesKpis.count()}")

println("DATA QUALITY TABLE LOADING COMPLETED")

// COMMAND ----------

println("===== BRONZE TO SILVER RECONCILIATION =====")

val datasets = Seq(
  ("customers", bronzeCustomers, silverCustomers, 5L, 2L),
  ("products", bronzeProducts, silverProducts, 4L, 2L),
  ("orders", bronzeOrders, silverOrders, 61L, 10L)
)

val reconciliationResults = datasets.map {
  case (name, bronzeDF, silverDF, expectedRejected, expectedDuplicates) =>

    val bronzeCount = bronzeDF.count()
    val silverCount = silverDF.count()

    val rejectedCount = spark.read
      .format("delta")
      .load(s"$rejectedBase/$name")
      .count()

    val summaryDF = spark.read
      .format("delta")
      .load(s"$rejectedBase/deduplication_summary/$name")

    println(s"\n===== $name =====")
    println(s"Bronze: $bronzeCount")
    println(s"Silver: $silverCount")
    println(s"Rejected: $rejectedCount")
    println(s"Expected duplicate extras: $expectedDuplicates")

    summaryDF.show(false)

    assert(
      rejectedCount == expectedRejected,
      s"$name rejected count mismatch"
    )

    assert(
      bronzeCount == silverCount + rejectedCount + expectedDuplicates,
      s"$name reconciliation failed"
    )

    println(s"$name RECONCILIATION PASSED")

    (
      name,
      bronzeCount,
      silverCount,
      rejectedCount,
      expectedDuplicates
    )
}

println("\n===== OVERALL RECONCILIATION =====")

val totalBronze = reconciliationResults.map(_._2).sum
val totalSilver = reconciliationResults.map(_._3).sum
val totalRejected = reconciliationResults.map(_._4).sum
val totalDuplicates = reconciliationResults.map(_._5).sum

println(s"Bronze: $totalBronze")
println(s"Silver: $totalSilver")
println(s"Rejected: $totalRejected")
println(s"Duplicate extras: $totalDuplicates")

assert(
  totalBronze == totalSilver + totalRejected + totalDuplicates,
  "Overall reconciliation failed"
)

println("ALL BRONZE TO SILVER RECONCILIATIONS PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== SILVER DATA INTEGRITY CHECKS =====")

val silverTables = Seq(
  ("customers", silverCustomers, "customer_id"),
  ("products", silverProducts, "product_id"),
  ("orders", silverOrders, "order_id")
)

silverTables.foreach { case (name, df, primaryKey) =>

  println(s"\n===== CHECKING $name =====")

  val totalRows = df.count()

  val nullOrBlankKeys = df.filter(
    col(primaryKey).isNull ||
    length(trim(col(primaryKey))) === 0
  ).count()

  val duplicateKeys = df
    .groupBy(col(primaryKey))
    .count()
    .filter(col("count") > 1)
    .count()

  println(s"Total rows: $totalRows")
  println(s"Null/blank primary keys: $nullOrBlankKeys")
  println(s"Duplicate primary keys: $duplicateKeys")

  assert(
    nullOrBlankKeys == 0,
    s"$name contains null or blank primary keys"
  )

  assert(
    duplicateKeys == 0,
    s"$name contains duplicate primary keys"
  )

  println(s"$name PRIMARY KEY CHECK PASSED")
}

println("\n===== FOREIGN KEY INTEGRITY =====")

val invalidCustomerReferences = silverOrders
  .join(
    silverCustomers.select("customer_id"),
    Seq("customer_id"),
    "left_anti"
  )
  .count()

val invalidProductReferences = silverOrders
  .join(
    silverProducts.select("product_id"),
    Seq("product_id"),
    "left_anti"
  )
  .count()

println(s"Invalid customer references: $invalidCustomerReferences")
println(s"Invalid product references: $invalidProductReferences")

assert(
  invalidCustomerReferences == 0,
  "Orders contain invalid customer references"
)

assert(
  invalidProductReferences == 0,
  "Orders contain invalid product references"
)

println("FOREIGN KEY INTEGRITY PASSED")

println("\n===== BUSINESS RULE CHECKS =====")

val invalidQuantities = silverOrders
  .filter(
    col("quantity").isNull ||
    col("quantity") <= 0
  )
  .count()

val invalidPrices = silverProducts
  .filter(
    col("unit_price").isNull ||
    col("unit_price") <= 0
  )
  .count()

val invalidOrderDates = silverOrders
  .filter(col("order_date").isNull)
  .count()

println(s"Invalid order quantities: $invalidQuantities")
println(s"Invalid product prices: $invalidPrices")
println(s"Null order dates: $invalidOrderDates")

assert(invalidQuantities == 0, "Invalid order quantities found")
assert(invalidPrices == 0, "Invalid product prices found")
assert(invalidOrderDates == 0, "Null order dates found")

println("BUSINESS RULE CHECKS PASSED")

println("\nALL SILVER DATA INTEGRITY CHECKS PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== GOLD DATA QUALITY CHECKS =====")

val silverOrderCount = silverOrders.count()
val factOrderCount = factSales.count()

assert(
  silverOrderCount == factOrderCount,
  "Silver-to-Gold order count mismatch"
)

val duplicateFactOrders = factSales
  .groupBy("order_id")
  .count()
  .filter(col("count") > 1)
  .count()

assert(
  duplicateFactOrders == 0,
  "Duplicate order IDs in fact_sales"
)

println("SILVER TO GOLD ORDER RECONCILIATION PASSED")

println("\n===== REVENUE RECONCILIATION =====")

val factRevenue = factSales
  .agg(sum("revenue"))
  .first()
  .getDecimal(0)

val cityRevenue = salesByCity
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val productRevenue = salesByProduct
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val monthlyRevenue = monthlySales
  .agg(sum("total_revenue"))
  .first()
  .getDecimal(0)

val kpiRevenue = salesKpis
  .select("total_revenue")
  .first()
  .getDecimal(0)

println(s"Fact revenue:    $factRevenue")
println(s"City revenue:    $cityRevenue")
println(s"Product revenue: $productRevenue")
println(s"Monthly revenue: $monthlyRevenue")
println(s"KPI revenue:     $kpiRevenue")

assert(factRevenue.compareTo(cityRevenue) == 0,
  "City revenue mismatch")

assert(factRevenue.compareTo(productRevenue) == 0,
  "Product revenue mismatch")

assert(factRevenue.compareTo(monthlyRevenue) == 0,
  "Monthly revenue mismatch")

assert(factRevenue.compareTo(kpiRevenue) == 0,
  "KPI revenue mismatch")

println("ALL REVENUE RECONCILIATIONS PASSED")

println("\n===== KPI CONSISTENCY =====")

val factMetrics = factSales.agg(
  count("*").alias("total_orders"),

  sum(
    when(col("order_status") === "DELIVERED", 1)
      .otherwise(0)
  ).alias("delivered_orders"),

  sum(
    when(col("order_status") === "DELIVERED", col("quantity"))
      .otherwise(0)
  ).alias("units_sold"),

  countDistinct("customer_id").alias("active_customers"),

  countDistinct("product_id").alias("products_ordered")
).first()

val kpi = salesKpis.first()

val metrics = Seq(
  "total_orders",
  "delivered_orders",
  "units_sold",
  "active_customers",
  "products_ordered"
)

metrics.foreach { metric =>
  val factValue = factMetrics.getAs[Long](metric)
  val kpiValue = kpi.getAs[Long](metric)

  println(s"$metric: fact=$factValue, KPI=$kpiValue")

  assert(
    factValue == kpiValue,
    s"KPI mismatch for $metric"
  )
}

println("KPI CONSISTENCY PASSED")

println("\n===== REVENUE BUSINESS RULE =====")

val invalidRevenueRows = factSales.filter(
  col("revenue").isNull ||
  col("revenue") < 0 ||
  (
    col("order_status") =!= "DELIVERED" &&
    col("revenue") =!= 0
  )
).count()

println(s"Invalid revenue rows: $invalidRevenueRows")

assert(
  invalidRevenueRows == 0,
  "Revenue business rule violated"
)

println("REVENUE BUSINESS RULE PASSED")

println("\nALL GOLD DATA QUALITY CHECKS PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== DYNAMIC DEDUPLICATION AUDIT =====")

val auditDatasets = Seq(
  ("customers", bronzeCustomers, silverCustomers, "customer_id"),
  ("products", bronzeProducts, silverProducts, "product_id"),
  ("orders", bronzeOrders, silverOrders, "order_id")
)

val auditResults = auditDatasets.map {
  case (name, bronzeDF, silverDF, primaryKey) =>

    println(s"\n===== AUDITING $name =====")

    val rejectedDF = spark.read
      .format("delta")
      .load(s"$rejectedBase/$name")

    val duplicateAuditDF = spark.read
      .format("delta")
      .load(s"$rejectedBase/duplicate_audit/$name")

    val duplicateSummaryDF = spark.read
      .format("delta")
      .load(s"$rejectedBase/deduplication_summary/$name")

    val bronzeCount = bronzeDF.count()
    val silverCount = silverDF.count()
    val rejectedCount = rejectedDF.count()

    val duplicateExtras = duplicateSummaryDF
      .agg(
        coalesce(
          sum(col("removed_extras")),
          lit(0L)
        ).alias("duplicate_extras")
      )
      .first()
      .getAs[Long]("duplicate_extras")

    val summaryDuplicateRows = duplicateSummaryDF
      .agg(
        coalesce(
          sum(col("total_records")),
          lit(0L)
        ).alias("duplicate_rows")
      )
      .first()
      .getAs[Long]("duplicate_rows")

    val auditRowCount = duplicateAuditDF.count()

    val invalidSummaryRows = duplicateSummaryDF
      .filter(
        col(primaryKey).isNull ||
        col("total_records") <= 1 ||
        col("removed_extras") =!= col("total_records") - 1
      )
      .count()

    println(s"Bronze: $bronzeCount")
    println(s"Silver: $silverCount")
    println(s"Rejected: $rejectedCount")
    println(s"Duplicate audit rows: $auditRowCount")
    println(s"Duplicate extras: $duplicateExtras")

    assert(
      invalidSummaryRows == 0,
      s"$name has invalid deduplication summary rows"
    )

    assert(
      auditRowCount == summaryDuplicateRows,
      s"$name duplicate audit row count mismatch"
    )

    assert(
      bronzeCount == silverCount + rejectedCount + duplicateExtras,
      s"$name dynamic reconciliation failed"
    )

    println(s"$name DYNAMIC AUDIT PASSED")

    (
      name,
      bronzeCount,
      silverCount,
      rejectedCount,
      duplicateExtras
    )
}

println("\n===== OVERALL DYNAMIC RECONCILIATION =====")

val totalBronzeDynamic = auditResults.map(_._2).sum
val totalSilverDynamic = auditResults.map(_._3).sum
val totalRejectedDynamic = auditResults.map(_._4).sum
val totalDuplicateExtrasDynamic = auditResults.map(_._5).sum

println(s"Bronze: $totalBronzeDynamic")
println(s"Silver: $totalSilverDynamic")
println(s"Rejected: $totalRejectedDynamic")
println(s"Duplicate extras: $totalDuplicateExtrasDynamic")

assert(
  totalBronzeDynamic ==
    totalSilverDynamic + totalRejectedDynamic + totalDuplicateExtrasDynamic,
  "Overall dynamic reconciliation failed"
)

println("ALL DYNAMIC DEDUPLICATION AUDITS PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._

println("===== GENERATING DATA QUALITY REPORT =====")

val dqStorage = "nexusretail2026"

val dqBronze = s"abfss://bronze@$dqStorage.dfs.core.windows.net"
val dqSilver = s"abfss://curated@$dqStorage.dfs.core.windows.net"
val dqGold = s"abfss://analytics@$dqStorage.dfs.core.windows.net"
val dqRejected = s"abfss://rejected@$dqStorage.dfs.core.windows.net"

def loadDQ(path: String) =
  spark.read.format("delta").load(path)

val dqBronzeCustomers = loadDQ(s"$dqBronze/customers")
val dqBronzeProducts = loadDQ(s"$dqBronze/products")
val dqBronzeOrders = loadDQ(s"$dqBronze/orders")

val dqSilverCustomers = loadDQ(s"$dqSilver/customers")
val dqSilverProducts = loadDQ(s"$dqSilver/products")
val dqSilverOrders = loadDQ(s"$dqSilver/orders")

val dqFact = loadDQ(s"$dqGold/fact_sales")
val dqCity = loadDQ(s"$dqGold/sales_by_city")
val dqProduct = loadDQ(s"$dqGold/sales_by_product")
val dqMonthly = loadDQ(s"$dqGold/monthly_sales")
val dqKpis = loadDQ(s"$dqGold/sales_kpis")

val dqSources = Seq(
  ("customers", dqBronzeCustomers, dqSilverCustomers),
  ("products", dqBronzeProducts, dqSilverProducts),
  ("orders", dqBronzeOrders, dqSilverOrders)
)

val dqReconciliationPassed = dqSources.forall {
  case (name, bronzeDF, silverDF) =>

    val rejectedCount =
      loadDQ(s"$dqRejected/$name").count()

    val duplicateSummary =
      loadDQ(s"$dqRejected/deduplication_summary/$name")

    val duplicateExtras = duplicateSummary
      .agg(
        coalesce(sum("removed_extras"), lit(0L))
          .alias("extras")
      )
      .first()
      .getAs[Long]("extras")

    bronzeDF.count() ==
      silverDF.count() + rejectedCount + duplicateExtras
}

def dqUnique(df: org.apache.spark.sql.DataFrame, key: String): Boolean = {
  val validKeys = df.filter(
    col(key).isNotNull &&
    length(trim(col(key))) > 0
  ).count()

  validKeys == df.count() &&
    df.select(key).distinct().count() == df.count()
}

def dqRevenue(df: org.apache.spark.sql.DataFrame, column: String) =
  df.agg(sum(col(column))).first().getDecimal(0)

val dqFactRevenue = dqRevenue(dqFact, "revenue")

val dqInvalidRevenue = dqFact.filter(
  col("revenue").isNull ||
  col("revenue") < 0 ||
  (
    col("order_status") =!= "DELIVERED" &&
    col("revenue") =!= 0
  )
).count()

val dqChecks = Seq(
  ("bronze_silver_reconciliation", dqReconciliationPassed),
  ("silver_customers_unique", dqUnique(dqSilverCustomers, "customer_id")),
  ("silver_products_unique", dqUnique(dqSilverProducts, "product_id")),
  ("silver_orders_unique", dqUnique(dqSilverOrders, "order_id")),
  ("silver_gold_order_reconciliation", dqSilverOrders.count() == dqFact.count()),
  ("city_revenue_reconciliation",
    dqFactRevenue.compareTo(dqRevenue(dqCity, "total_revenue")) == 0),
  ("product_revenue_reconciliation",
    dqFactRevenue.compareTo(dqRevenue(dqProduct, "total_revenue")) == 0),
  ("monthly_revenue_reconciliation",
    dqFactRevenue.compareTo(dqRevenue(dqMonthly, "total_revenue")) == 0),
  ("kpi_revenue_reconciliation",
    dqFactRevenue.compareTo(dqKpis.first().getAs[java.math.BigDecimal]("total_revenue")) == 0),
  ("revenue_business_rule", dqInvalidRevenue == 0)
)

val dqRunId = java.util.UUID.randomUUID().toString

val dqReport = dqChecks
  .map { case (name, passed) =>
    (dqRunId, name, if (passed) "PASS" else "FAIL")
  }
  .toDF("run_id", "check_name", "status")
  .withColumn("checked_at", current_timestamp())

dqReport.show(false)

val dqFailed = dqChecks.count(!_._2)

println(s"Run ID: $dqRunId")
println(s"Total checks: ${dqChecks.size}")
println(s"Failed checks: $dqFailed")

dqReport.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .save(s"$dqGold/data_quality_report")

println("DATA QUALITY REPORT SAVED")

assert(dqFailed == 0, s"$dqFailed DATA QUALITY CHECKS FAILED")

println("ALL DATA QUALITY CHECKS PASSED")