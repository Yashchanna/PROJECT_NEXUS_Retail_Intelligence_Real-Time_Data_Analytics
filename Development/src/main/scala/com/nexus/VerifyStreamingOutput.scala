
package com.nexus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object VerifyStreamingOutput {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NEXUS_Output_Verification")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val base = "output"

    val bronze = spark.read.parquet(s"$base/bronze")
    val valid = spark.read.parquet(s"$base/valid_events")
    val rejected = spark.read.parquet(s"$base/rejected_events")

    println("=== PROJECT NEXUS DATA QUALITY REPORT ===")

    println(s"Bronze records: ${bronze.count()}")
    println(s"Valid records: ${valid.count()}")
    println(s"Rejected records: ${rejected.count()}")

    println("=== DUPLICATE EVENT IDS IN VALID DATA ===")
    valid.groupBy("event_id")
      .count()
      .filter(col("count") > 1)
      .show(false)

    println("=== EVENT TYPE DISTRIBUTION ===")
    valid.groupBy("event_type")
      .count()
      .show(false)

    println("=== REJECTED EVENT REASONS ===")
    rejected.printSchema()
    rejected.show(20, truncate = false)

    spark.stop()
  }
}
