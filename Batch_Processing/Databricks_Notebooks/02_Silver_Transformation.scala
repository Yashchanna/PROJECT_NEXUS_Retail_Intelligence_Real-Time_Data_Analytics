// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import io.delta.tables.DeltaTable

val storageAccount = "nexusretail2026"

val bronzeBase =
  s"abfss://bronze@$storageAccount.dfs.core.windows.net"

val silverBase =
  s"abfss://curated@$storageAccount.dfs.core.windows.net"

val rejectedBase =
  s"abfss://rejected@$storageAccount.dfs.core.windows.net"

val bronzeCustomers = spark.read.format("delta")
  .load(s"$bronzeBase/customers")

val bronzeOrders = spark.read.format("delta")
  .load(s"$bronzeBase/orders")

val bronzeProducts = spark.read.format("delta")
  .load(s"$bronzeBase/products")

println("===== BRONZE INPUT VALIDATION =====")

println(s"Customers: ${bronzeCustomers.count()}")
println(s"Orders: ${bronzeOrders.count()}")
println(s"Products: ${bronzeProducts.count()}")

println("\n===== BRONZE SCHEMAS =====")

bronzeCustomers.printSchema()
bronzeOrders.printSchema()
bronzeProducts.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// 1. Clean customer fields
val cleanedCustomers = bronzeCustomers
  .withColumn("customer_id", upper(trim(col("customer_id"))))
  .withColumn("customer_name", trim(col("customer_name")))
  .withColumn("email", lower(trim(col("email"))))
  .withColumn("city", initcap(lower(trim(col("city")))))
  .withColumn("signup_date", to_date(col("signup_date")))

// 2. Validate customer records
val validatedCustomers = cleanedCustomers
  .withColumn(
    "_reject_reason",
    when(
      col("customer_id").isNull ||
      (col("customer_id") === ""),
      "INVALID_CUSTOMER_ID"
    )
    .when(
      col("customer_name").isNull ||
      (col("customer_name") === ""),
      "INVALID_CUSTOMER_NAME"
    )
    .when(
      col("email").isNull ||
      !col("email").rlike("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"),
      "INVALID_EMAIL"
    )
    .when(
      col("city").isNull ||
      (col("city") === ""),
      "INVALID_CITY"
    )
    .when(
      col("signup_date").isNull,
      "INVALID_SIGNUP_DATE"
    )
    .otherwise(lit(null).cast("string"))
  )

// 3. Separate valid and rejected records
val validCustomerCandidates = validatedCustomers
  .filter(col("_reject_reason").isNull)

val rejectedCustomers = validatedCustomers
  .filter(col("_reject_reason").isNotNull)

// 4. Deduplicate valid customer IDs
val customerWindow = Window
  .partitionBy("customer_id")
  .orderBy(
    col("_ingestion_timestamp").desc_nulls_last,
    col("_source_file").asc_nulls_last,
    col("customer_name").asc_nulls_last,
    col("email").asc_nulls_last,
    col("city").asc_nulls_last
  )

val rankedCustomers = validCustomerCandidates
  .withColumn("_row_number", row_number().over(customerWindow))

val silverCustomers = rankedCustomers
  .filter(col("_row_number") === 1)
  .select(
    "customer_id",
    "customer_name",
    "email",
    "city",
    "signup_date"
  )

val duplicateCustomerExtras = rankedCustomers
  .filter(col("_row_number") > 1)

// 5. Verify counts
println("===== CUSTOMER TRANSFORMATION =====")
println(s"Bronze customers: ${bronzeCustomers.count()}")
println(s"Silver customers: ${silverCustomers.count()}")
println(s"Rejected customers: ${rejectedCustomers.count()}")
println(s"Duplicate extras: ${duplicateCustomerExtras.count()}")

assert(
  bronzeCustomers.count() ==
    silverCustomers.count() +
    rejectedCustomers.count() +
    duplicateCustomerExtras.count(),
  "Customer count reconciliation failed"
)

println("CUSTOMER RECONCILIATION PASSED")

silverCustomers.show(5, false)

rejectedCustomers
  .groupBy("_reject_reason")
  .count()
  .show(false)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// 1. Clean product fields
val cleanedProducts = bronzeProducts
  .withColumn("product_id", upper(trim(col("product_id"))))
  .withColumn("product_name", trim(col("product_name")))
  .withColumn("category", initcap(lower(trim(col("category")))))
  .withColumn("active", lower(trim(col("active"))))

// 2. Validate products
val validatedProducts = cleanedProducts
  .withColumn(
    "_reject_reason",
    when(
      col("product_id").isNull ||
      (col("product_id") === ""),
      "INVALID_PRODUCT_ID"
    )
    .when(
      col("product_name").isNull ||
      (col("product_name") === ""),
      "INVALID_PRODUCT_NAME"
    )
    .when(
      col("category").isNull ||
      (col("category") === ""),
      "INVALID_CATEGORY"
    )
    .when(
      col("unit_price").isNull ||
      isnan(col("unit_price")) ||
      (col("unit_price") <= 0),
      "INVALID_UNIT_PRICE"
    )
    .when(
      col("active").isNull ||
      !col("active").isin("true", "false"),
      "INVALID_ACTIVE_STATUS"
    )
    .otherwise(lit(null).cast("string"))
  )

// 3. Separate valid and rejected products
val validProductCandidates = validatedProducts
  .filter(col("_reject_reason").isNull)

val rejectedProducts = validatedProducts
  .filter(col("_reject_reason").isNotNull)

// 4. Deduplicate valid product IDs
val productWindow = Window
  .partitionBy("product_id")
  .orderBy(
    col("_ingestion_timestamp").desc_nulls_last,
    col("_source_file").asc_nulls_last,
    col("product_name").asc_nulls_last,
    col("category").asc_nulls_last,
    col("unit_price").asc_nulls_last
  )

val rankedProducts = validProductCandidates
  .withColumn("_row_number", row_number().over(productWindow))

val silverProducts = rankedProducts
  .filter(col("_row_number") === 1)
  .select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("unit_price").cast("decimal(18,2)").alias("unit_price"),
    col("active").cast("boolean").alias("active")
  )

val duplicateProductExtras = rankedProducts
  .filter(col("_row_number") > 1)

// 5. Verify reconciliation
val bronzeProductCount = bronzeProducts.count()
val silverProductCount = silverProducts.count()
val rejectedProductCount = rejectedProducts.count()
val duplicateProductCount = duplicateProductExtras.count()

println("===== PRODUCT TRANSFORMATION =====")
println(s"Bronze products: $bronzeProductCount")
println(s"Silver products: $silverProductCount")
println(s"Rejected products: $rejectedProductCount")
println(s"Duplicate extras: $duplicateProductCount")

assert(
  bronzeProductCount ==
    silverProductCount +
    rejectedProductCount +
    duplicateProductCount,
  "Product reconciliation failed"
)

println("PRODUCT RECONCILIATION PASSED")

silverProducts.show(5, false)

rejectedProducts
  .groupBy("_reject_reason")
  .count()
  .show(false)

// COMMAND ----------

println("===== ORIGINAL ACTIVE VALUES =====")

bronzeProducts
  .groupBy("active")
  .count()
  .orderBy(desc("count"))
  .show(false)

println("===== ACTIVE STATUS SAMPLES =====")

bronzeProducts
  .select("product_id", "product_name", "active")
  .show(27, false)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val cleanedProducts = bronzeProducts
  .withColumn("product_id", upper(trim(col("product_id"))))
  .withColumn("product_name", trim(col("product_name")))
  .withColumn("category", initcap(lower(trim(col("category")))))
  .withColumn("active_raw", upper(trim(col("active"))))
  .withColumn(
    "active",
    when(col("active_raw").isin("Y", "YES", "TRUE", "1"), lit(true))
      .when(col("active_raw").isin("N", "NO", "FALSE", "0"), lit(false))
      .otherwise(lit(null).cast("boolean"))
  )

val validatedProducts = cleanedProducts
  .withColumn(
    "_reject_reason",
    when(
      col("product_id").isNull ||
      (col("product_id") === ""),
      "INVALID_PRODUCT_ID"
    )
    .when(
      col("product_name").isNull ||
      (col("product_name") === ""),
      "INVALID_PRODUCT_NAME"
    )
    .when(
      col("category").isNull ||
      (col("category") === ""),
      "INVALID_CATEGORY"
    )
    .when(
      col("unit_price").isNull ||
      isnan(col("unit_price")) ||
      (col("unit_price") <= 0),
      "INVALID_UNIT_PRICE"
    )
    .when(
      col("active").isNull,
      "INVALID_ACTIVE_STATUS"
    )
    .otherwise(lit(null).cast("string"))
  )

val validProductCandidates = validatedProducts
  .filter(col("_reject_reason").isNull)

val rejectedProducts = validatedProducts
  .filter(col("_reject_reason").isNotNull)

val productWindow = Window
  .partitionBy("product_id")
  .orderBy(
    col("_ingestion_timestamp").desc_nulls_last,
    col("_source_file").asc_nulls_last,
    col("product_name").asc_nulls_last,
    col("category").asc_nulls_last,
    col("unit_price").asc_nulls_last
  )

val rankedProducts = validProductCandidates
  .withColumn("_row_number", row_number().over(productWindow))

val silverProducts = rankedProducts
  .filter(col("_row_number") === 1)
  .select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("unit_price").cast("decimal(18,2)").alias("unit_price"),
    col("active")
  )

val duplicateProductExtras = rankedProducts
  .filter(col("_row_number") > 1)

val bronzeProductCount = bronzeProducts.count()
val silverProductCount = silverProducts.count()
val rejectedProductCount = rejectedProducts.count()
val duplicateProductCount = duplicateProductExtras.count()

println("===== PRODUCT TRANSFORMATION =====")
println(s"Bronze products: $bronzeProductCount")
println(s"Silver products: $silverProductCount")
println(s"Rejected products: $rejectedProductCount")
println(s"Duplicate extras: $duplicateProductCount")

assert(
  bronzeProductCount ==
    silverProductCount +
    rejectedProductCount +
    duplicateProductCount,
  "Product reconciliation failed"
)

assert(silverProductCount > 0, "No valid products found")

println("PRODUCT RECONCILIATION PASSED")

silverProducts.show(5, false)

rejectedProducts
  .groupBy("_reject_reason")
  .count()
  .show(false)

// COMMAND ----------

println("===== REJECTED PRODUCT DETAILS =====")

rejectedProducts
  .select(
    "product_id",
    "product_name",
    "category",
    "unit_price",
    "active_raw",
    "_reject_reason"
  )
  .show(false)

println("===== REJECTED PRODUCT SOURCE RECORDS =====")

bronzeProducts
  .join(
    rejectedProducts.select("product_id").distinct(),
    Seq("product_id"),
    "inner"
  )
  .select(
    "product_id",
    "product_name",
    "category",
    "unit_price",
    "active"
  )
  .show(false)

// COMMAND ----------

import org.apache.spark.sql.functions._

val cleanedOrders = bronzeOrders
  .withColumn("order_id", upper(trim(col("order_id"))))
  .withColumn("customer_id", upper(trim(col("customer_id"))))
  .withColumn("product_id", upper(trim(col("product_id"))))
  .withColumn("quantity", trim(col("quantity")).cast("int"))
  .withColumn("order_date", to_date(col("order_date")))
  .withColumn("order_status", upper(trim(col("order_status"))))

val validatedOrders = cleanedOrders
  .withColumn(
    "_reject_reason",
    when(
      col("order_id").isNull || col("order_id") === "",
      "INVALID_ORDER_ID"
    )
    .when(
      col("customer_id").isNull || col("customer_id") === "",
      "INVALID_CUSTOMER_ID"
    )
    .when(
      col("product_id").isNull || col("product_id") === "",
      "INVALID_PRODUCT_ID"
    )
    .when(
      col("quantity").isNull || col("quantity") <= 0,
      "INVALID_QUANTITY"
    )
    .when(
      col("order_date").isNull,
      "INVALID_ORDER_DATE"
    )
    .when(
      col("order_status").isNull ||
      !col("order_status").isin(
        "PLACED",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
        "RETURNED"
      ),
      "INVALID_ORDER_STATUS"
    )
    .when(
      col("last_updated").isNull,
      "INVALID_LAST_UPDATED"
    )
    .otherwise(lit(null).cast("string"))
  )

val validOrderCandidates = validatedOrders
  .filter(col("_reject_reason").isNull)

val basicRejectedOrders = validatedOrders
  .filter(col("_reject_reason").isNotNull)

println("===== ORDER BASIC VALIDATION =====")
println(s"Bronze orders: ${bronzeOrders.count()}")
println(s"Basic valid candidates: ${validOrderCandidates.count()}")
println(s"Basic rejected orders: ${basicRejectedOrders.count()}")

assert(
  bronzeOrders.count() ==
    validOrderCandidates.count() + basicRejectedOrders.count(),
  "Basic order reconciliation failed"
)

println("BASIC ORDER RECONCILIATION PASSED")

basicRejectedOrders
  .groupBy("_reject_reason")
  .count()
  .orderBy(desc("count"))
  .show(false)

println("===== ORD000081 CHECK =====")

validatedOrders
  .filter(col("order_id") === "ORD000081")
  .select("order_id", "order_status", "_reject_reason")
  .show(false)

// COMMAND ----------

import org.apache.spark.sql.functions._

val validCustomerIds = silverCustomers
  .select("customer_id")
  .distinct()

val validProductIds = silverProducts
  .select("product_id")
  .distinct()

// Check customer foreign keys
val ordersWithCustomerCheck = validOrderCandidates
  .join(
    validCustomerIds.withColumn(
      "_customer_exists",
      lit(true)
    ),
    Seq("customer_id"),
    "left"
  )

// Check product foreign keys
val ordersWithForeignKeys = ordersWithCustomerCheck
  .join(
    validProductIds.withColumn(
      "_product_exists",
      lit(true)
    ),
    Seq("product_id"),
    "left"
  )
  .withColumn(
    "_reject_reason",
    when(
      col("_customer_exists").isNull,
      "CUSTOMER_NOT_FOUND"
    )
    .when(
      col("_product_exists").isNull,
      "PRODUCT_NOT_FOUND"
    )
    .otherwise(lit(null).cast("string"))
  )
  .drop("_customer_exists", "_product_exists")

val foreignKeyValidOrders = ordersWithForeignKeys
  .filter(col("_reject_reason").isNull)

val foreignKeyRejectedOrders = ordersWithForeignKeys
  .filter(col("_reject_reason").isNotNull)

// Verify counts
val basicValidCount = validOrderCandidates.count()
val fkValidCount = foreignKeyValidOrders.count()
val fkRejectedCount = foreignKeyRejectedOrders.count()

println("===== ORDER FOREIGN KEY VALIDATION =====")
println(s"Basic valid orders: $basicValidCount")
println(s"Foreign-key valid orders: $fkValidCount")
println(s"Foreign-key rejected orders: $fkRejectedCount")

assert(
  basicValidCount == fkValidCount + fkRejectedCount,
  "Foreign key reconciliation failed"
)

println("FOREIGN KEY RECONCILIATION PASSED")

foreignKeyRejectedOrders
  .groupBy("_reject_reason")
  .count()
  .orderBy(desc("count"))
  .show(false)

println("===== SAMPLE REJECTED ORDERS =====")

foreignKeyRejectedOrders
  .select(
    "order_id",
    "customer_id",
    "product_id",
    "_reject_reason"
  )
  .show(10, false)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val orderWindow = Window
  .partitionBy("order_id")
  .orderBy(
    col("last_updated").desc_nulls_last,
    col("_ingestion_timestamp").desc_nulls_last,
    col("_source_file").asc_nulls_last,
    col("customer_id").asc_nulls_last,
    col("product_id").asc_nulls_last,
    col("order_status").asc_nulls_last
  )

val rankedOrders = foreignKeyValidOrders
  .withColumn(
    "_row_number",
    row_number().over(orderWindow)
  )

val silverOrders = rankedOrders
  .filter(col("_row_number") === 1)
  .select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "order_date",
    "order_status",
    "last_updated"
  )

val duplicateOrderExtras = rankedOrders
  .filter(col("_row_number") > 1)

val fkValidOrderCount = foreignKeyValidOrders.count()
val silverOrderCount = silverOrders.count()
val duplicateOrderCount = duplicateOrderExtras.count()

println("===== ORDER DEDUPLICATION =====")

println(s"Foreign-key valid orders: $fkValidOrderCount")
println(s"Silver orders: $silverOrderCount")
println(s"Duplicate extras: $duplicateOrderCount")

assert(
  fkValidOrderCount == silverOrderCount + duplicateOrderCount,
  "Order deduplication reconciliation failed"
)

assert(
  silverOrders.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count() == 0,
  "Duplicate order IDs found in Silver"
)

println("ORDER DEDUPLICATION PASSED")

println("===== DUPLICATE ORDER IDS =====")

duplicateOrderExtras
  .groupBy("order_id")
  .count()
  .orderBy("order_id")
  .show(false)

println("===== FINAL ORDER RECONCILIATION =====")

val totalRejectedOrders =
  basicRejectedOrders.count() + foreignKeyRejectedOrders.count()

println(s"Bronze: ${bronzeOrders.count()}")
println(s"Silver: $silverOrderCount")
println(s"Rejected: $totalRejectedOrders")
println(s"Duplicate extras: $duplicateOrderCount")

assert(
  bronzeOrders.count() ==
    silverOrderCount + totalRejectedOrders + duplicateOrderCount,
  "Final order reconciliation failed"
)

println("FINAL ORDER RECONCILIATION PASSED")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== SAVING SILVER TABLES =====")

val silverDatasets = Seq(
  ("customers", silverCustomers),
  ("products", silverProducts),
  ("orders", silverOrders)
)

silverDatasets.foreach { case (name, df) =>
  val path = s"$silverBase/$name"

  df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)

  println(s"Silver $name saved: ${df.count()} rows")
}

println("===== SAVING REJECTED TABLES =====")

val allRejectedOrders = basicRejectedOrders
  .unionByName(foreignKeyRejectedOrders)

val rejectedDatasets = Seq(
  ("customers", rejectedCustomers),
  ("products", rejectedProducts),
  ("orders", allRejectedOrders)
)

rejectedDatasets.foreach { case (name, df) =>
  val path = s"$rejectedBase/$name"

  df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)

  println(s"Rejected $name saved: ${df.count()} rows")
}

println("===== VERIFYING SAVED TABLES =====")

silverDatasets.foreach { case (name, df) =>
  val actual = spark.read.format("delta")
    .load(s"$silverBase/$name")
    .count()

  val expected = df.count()

  assert(
    actual == expected,
    s"Silver $name verification failed"
  )

  println(s"Silver $name verified: $actual rows")
}

rejectedDatasets.foreach { case (name, df) =>
  val actual = spark.read.format("delta")
    .load(s"$rejectedBase/$name")
    .count()

  val expected = df.count()

  assert(
    actual == expected,
    s"Rejected $name verification failed"
  )

  println(s"Rejected $name verified: $actual rows")
}

println("SILVER AND REJECTED DELTA TABLES SAVED SUCCESSFULLY")

// COMMAND ----------

import org.apache.spark.sql.functions._

println("===== SAVING DUPLICATE AUDIT =====")

val duplicateDatasets = Seq(
  ("customers", rankedCustomers, "customer_id"),
  ("products", rankedProducts, "product_id"),
  ("orders", rankedOrders, "order_id")
)

duplicateDatasets.foreach { case (name, rankedDf, idColumn) =>

  val duplicateIds = rankedDf
    .groupBy(idColumn)
    .agg(count(lit(1)).alias("_duplicate_count"))
    .filter(col("_duplicate_count") > 1)

  val duplicateAudit = rankedDf
    .join(duplicateIds, Seq(idColumn), "inner")
    .withColumn(
      "_record_action",
      when(col("_row_number") === 1, lit("RETAINED"))
        .otherwise(lit("REMOVED_DUPLICATE"))
    )

  val deduplicationSummary = duplicateIds
    .withColumn(
      "removed_extras",
      col("_duplicate_count") - lit(1)
    )
    .withColumnRenamed(
      "_duplicate_count",
      "total_records"
    )

  val auditPath = s"$rejectedBase/duplicate_audit/$name"
  val summaryPath = s"$rejectedBase/deduplication_summary/$name"

  duplicateAudit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(auditPath)

  deduplicationSummary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(summaryPath)

  val auditCount = spark.read
    .format("delta")
    .load(auditPath)
    .count()

  val summaryDf = spark.read
    .format("delta")
    .load(summaryPath)

  val removedCount = summaryDf
    .agg(sum("removed_extras"))
    .first()
    .getLong(0)

  val expectedRemoved = rankedDf
    .filter(col("_row_number") > 1)
    .count()

  assert(
    removedCount == expectedRemoved,
    s"$name duplicate reconciliation failed"
  )

  println(s"$name:")
  println(s"  Duplicate audit rows: $auditCount")
  println(s"  Duplicated IDs: ${summaryDf.count()}")
  println(s"  Removed extras: $removedCount")
  println("  AUDIT VERIFIED")
}

println("DUPLICATE AUDIT AND SUMMARY COMPLETED")