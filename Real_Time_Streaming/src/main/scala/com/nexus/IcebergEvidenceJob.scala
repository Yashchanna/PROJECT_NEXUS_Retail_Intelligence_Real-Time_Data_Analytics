package com.nexus

import java.nio.file.Paths
import org.apache.spark.sql.SparkSession

/** Print Iceberg snapshot, time-travel, and schema-evolution evidence. */
object IcebergEvidenceJob {
  def main(args: Array[String]): Unit = {
    val output = Paths.get(sys.env.getOrElse("NEXUS_OUTPUT_DIR", "output")).toAbsolutePath.normalize()
    val warehouse = sys.env.getOrElse("NEXUS_ICEBERG_WAREHOUSE", output.resolve("iceberg-warehouse").toUri.toString)
    val spark = SparkSession.builder().appName("PROJECT_NEXUS_Iceberg_Evidence")
      .master(sys.env.getOrElse("NEXUS_SPARK_MASTER", "local[2]"))
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.nexus", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.nexus.type", "hadoop")
      .config("spark.sql.catalog.nexus.warehouse", warehouse)
      .getOrCreate()

    println("=== ICEBERG SNAPSHOT HISTORY ===")
    val snapshotFrame = spark.sql("SELECT snapshot_id, committed_at, operation FROM nexus.retail.window_analytics.snapshots ORDER BY committed_at")
    snapshotFrame.show(100, truncate = false)
    val ids = snapshotFrame.select("snapshot_id").collect().map(_.getLong(0))
    if (ids.nonEmpty) {
      println(s"=== TIME TRAVEL TO FIRST SNAPSHOT ${ids.head} ===")
      spark.sql(s"SELECT * FROM nexus.retail.window_analytics VERSION AS OF ${ids.head}")
        .show(100, truncate = false)
    }
    if (ids.length < 2) println("Publish updated data again to show a comparison between multiple snapshots.")

    // Demonstrate additive schema evolution without changing current publisher's MERGE column list.
    val columns = spark.table("nexus.retail.window_analytics").columns.toSet
    if (!columns.contains("sales_channel")) {
      spark.sql("ALTER TABLE nexus.retail.window_analytics ADD COLUMN sales_channel STRING")
    }
    println("=== ICEBERG EVOLVED SCHEMA ===")
    spark.sql("DESCRIBE TABLE nexus.retail.window_analytics").show(100, truncate = false)
    spark.stop()
  }
}
