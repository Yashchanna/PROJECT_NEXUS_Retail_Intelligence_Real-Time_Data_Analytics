package com.nexus

import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object InspectOutputs {
  def main(args: Array[String]): Unit = {
    val output = Paths.get(sys.env.getOrElse("NEXUS_OUTPUT_DIR", "output")).toAbsolutePath.normalize()
    val spark = SparkSession.builder()
      .appName("PROJECT_NEXUS_Inspect_Outputs")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = spark.read.parquet(output.resolve("bronze").toString)
    val valid = spark.read.parquet(output.resolve("valid_events").toString)
    val rejected = spark.read.parquet(output.resolve("rejected_events").toString)

    println(s"Kafka source messages persisted: ${raw.count()}")
    println(s"Valid deduplicated events: ${valid.count()}")
    println(s"Rejected messages persisted: ${rejected.count()}")
    println("Valid event types:")
    valid.groupBy("event_type").count().orderBy("event_type").show(false)
    println("Rejected records and reasons:")
    rejected.select("event_id", "order_id", "reject_reason").show(100, false)
    println("Duplicated accepted event IDs (should be zero for one initial replay):")
    valid.groupBy("event_id").count().filter(col("count") > 1).show(false)
    spark.stop()
  }
}
