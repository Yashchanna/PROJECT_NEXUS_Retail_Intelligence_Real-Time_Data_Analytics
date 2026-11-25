// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val storageAccount = "nexusretail2026"

val rawBase =
  s"abfss://raw@$storageAccount.dfs.core.windows.net"

val bronzeBase =
  s"abfss://bronze@$storageAccount.dfs.core.windows.net"

val datasets = Seq("customers", "orders", "products")

println("PROJECT NEXUS - BRONZE INGESTION")
println(s"Storage account: $storageAccount")
println(s"Datasets: ${datasets.mkString(", ")}")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val customerSchema = StructType(Seq(
  StructField("customer_id", StringType, true),
  StructField("customer_name", StringType, true),
  StructField("email", StringType, true),
  StructField("city", StringType, true),
  StructField("signup_date", StringType, true)
))

val orderSchema = StructType(Seq(
  StructField("order_id", StringType, true),
  StructField("customer_id", StringType, true),
  StructField("product_id", StringType, true),
  StructField("quantity", StringType, true),
  StructField("order_date", StringType, true),
  StructField("order_status", StringType, true),
  StructField("last_updated", TimestampType, true)
))

val productSchema = StructType(Seq(
  StructField("product_id", StringType, true),
  StructField("product_name", StringType, true),
  StructField("category", StringType, true),
  StructField("unit_price", DoubleType, true),
  StructField("active", StringType, true)
))

val schemas = Map(
  "customers" -> customerSchema,
  "orders" -> orderSchema,
  "products" -> productSchema
)

datasets.foreach { dataset =>

  val sourcePath = s"$rawBase/$dataset/$dataset.csv"
  val targetPath = s"$bronzeBase/$dataset"

  val rawDF = spark.read
    .option("header", "true")
    .schema(schemas(dataset))
    .csv(sourcePath)

  val bronzeDF = rawDF
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())

  bronzeDF.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(targetPath)

  println(s"$dataset: ${bronzeDF.count()} rows ingested")
}

println("BRONZE INGESTION COMPLETED")