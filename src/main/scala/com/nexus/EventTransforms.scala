package com.nexus

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Schema and data-quality logic shared by streaming and unit tests. */
object EventTransforms {
  val eventSchema: StructType = new StructType()
    .add("event_id", StringType)
    .add("order_id", StringType)
    .add("customer_id", StringType)
    .add("product_id", StringType)
    .add("quantity", IntegerType)
    .add("unit_price", DecimalType(14, 2))
    .add("event_type", StringType)
    .add("event_time", StringType)
    .add("ingest_time", StringType)
    .add("sales_channel", StringType)
    .add("discount_amount", DecimalType(14, 2))

  def parse(raw: DataFrame): DataFrame = raw
    .withColumn("payload", from_json(col("raw_json"), eventSchema))
    .selectExpr("kafka_key", "raw_json", "topic", "partition", "offset", "kafka_timestamp", "payload.*")
    .withColumn("event_time_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
    .withColumn("ingest_time_ts", to_timestamp(col("ingest_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
    .withColumn("discount_amount", coalesce(col("discount_amount"), lit(0).cast(DecimalType(14, 2))))
    .withColumn("gross_amount", col("quantity") * col("unit_price"))
    .withColumn("net_amount", col("gross_amount") - col("discount_amount"))

  def withRejectReasons(decoded: DataFrame): DataFrame = {
    val createdOrUpdated = col("event_type").isin("ORDER_CREATED", "ORDER_UPDATED")
    decoded.withColumn("reject_reason", concat_ws("; ", array(
      when(col("event_id").isNull || !col("event_id").rlike("^EVT[0-9]{6}$"), lit("INVALID_EVENT_ID")),
      when(col("order_id").isNull || !col("order_id").rlike("^ORD[0-9]{6}$"), lit("INVALID_ORDER_ID")),
      when(col("event_type").isNull || !col("event_type").isin("ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED"), lit("INVALID_EVENT_TYPE")),
      when(col("event_time_ts").isNull || col("ingest_time_ts").isNull, lit("INVALID_TIMESTAMPS")),
      when(col("event_time_ts") > expr("ingest_time_ts + INTERVAL 5 MINUTES"), lit("FUTURE_EVENT_TIME")),
      when(createdOrUpdated && (col("customer_id").isNull || !col("customer_id").rlike("^CUST[0-9]{3}$")), lit("INVALID_CUSTOMER_ID")),
      when(createdOrUpdated && (col("product_id").isNull || !col("product_id").rlike("^PRD[0-9]{3}$")), lit("INVALID_PRODUCT_ID")),
      when(createdOrUpdated && (col("quantity").isNull || col("quantity") <= 0), lit("INVALID_QUANTITY")),
      when(createdOrUpdated && (col("unit_price").isNull || col("unit_price") <= 0), lit("INVALID_PRICE")),
      when(col("discount_amount") < 0, lit("NEGATIVE_DISCOUNT")),
      when(createdOrUpdated && col("discount_amount") > col("gross_amount"), lit("EXCESSIVE_DISCOUNT"))
    )))
  }

  def valid(errors: DataFrame): DataFrame = errors
    .filter(length(col("reject_reason")) === 0)
    .drop("reject_reason")
    .withColumnRenamed("event_time_ts", "event_ts")
    .withColumnRenamed("ingest_time_ts", "ingest_ts")
    .withColumn("date_key", date_format(col("event_ts"), "yyyyMMdd").cast("int"))
    .withColumn("event_net_amount", when(col("event_type") === "ORDER_CREATED", col("net_amount"))
      .otherwise(lit(null).cast(DecimalType(18, 2))))

  def rejected(errors: DataFrame): DataFrame = errors.filter(length(col("reject_reason")) > 0)

  def fiveMinuteWindows(uniqueEvents: DataFrame, watermark: String, interval: String): DataFrame = uniqueEvents
    .withWatermark("event_ts", watermark)
    .groupBy(window(col("event_ts"), interval))
    .agg(
      count(lit(1)).as("events"),
      sum(when(col("event_type") === "ORDER_CREATED", lit(1)).otherwise(lit(0))).as("new_orders"),
      sum(when(col("event_type") === "ORDER_UPDATED", lit(1)).otherwise(lit(0))).as("updates"),
      sum(when(col("event_type") === "ORDER_CANCELLED", lit(1)).otherwise(lit(0))).as("cancellations"),
      sum(when(col("event_type") === "ORDER_CREATED", col("quantity")).otherwise(lit(0))).as("new_units"),
      sum(when(col("event_type") === "ORDER_CREATED", col("net_amount"))
        .otherwise(lit(0).cast(DecimalType(18, 2)))).as("new_order_value")
    )
    .select(
      date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").as("window_start_utc"),
      date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").as("window_end_utc"),
      col("events"), col("new_orders"), col("updates"), col("cancellations"),
      col("new_units"), col("new_order_value")
    )
}
