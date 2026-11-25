package com.nexus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class EventTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("NexusEventRulesTest")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def validate(json: String) = {
    val session = spark
    import session.implicits._
    val raw = Seq(json).toDF("raw_json")
      .withColumn("kafka_key", lit("ORD900001"))
      .withColumn("topic", lit("retail-order-events"))
      .withColumn("partition", lit(0))
      .withColumn("offset", lit(0L))
      .withColumn("kafka_timestamp", current_timestamp())
    EventTransforms.withRejectReasons(EventTransforms.parse(raw))
  }

  test("valid created event is accepted and amount is calculated") {
    val row = validate("""{"event_id":"EVT000001","order_id":"ORD900001","customer_id":"CUST001","product_id":"PRD001","quantity":2,"unit_price":100.00,"event_type":"ORDER_CREATED","event_time":"2026-11-19T10:00:00Z","ingest_time":"2026-11-19T10:00:01Z","discount_amount":10.00}""")
    assert(EventTransforms.rejected(row).count() == 0L)
    val clean = EventTransforms.valid(row)
    assert(clean.count() == 1L)
    assert(clean.select("net_amount").head().getDecimal(0).doubleValue() == 190.0)
  }

  test("negative quantity is rejected") {
    val row = validate("""{"event_id":"EVT000002","order_id":"ORD900002","customer_id":"CUST001","product_id":"PRD001","quantity":-1,"unit_price":100,"event_type":"ORDER_CREATED","event_time":"2026-11-19T10:00:00Z","ingest_time":"2026-11-19T10:00:01Z"}""")
    assert(EventTransforms.rejected(row).head().getAs[String]("reject_reason").contains("INVALID_QUANTITY"))
  }

  test("unsupported event type is rejected") {
    val row = validate("""{"event_id":"EVT000003","order_id":"ORD900003","event_type":"ORDER_ERR","event_time":"2026-11-19T10:00:00Z","ingest_time":"2026-11-19T10:00:01Z"}""")
    assert(EventTransforms.rejected(row).head().getAs[String]("reject_reason").contains("INVALID_EVENT_TYPE"))
  }

  test("missing order identifier is rejected") {
    val row = validate("""{"event_id":"EVT000004","customer_id":"CUST001","product_id":"PRD001","quantity":2,"unit_price":100,"event_type":"ORDER_CREATED","event_time":"2026-11-19T10:00:00Z","ingest_time":"2026-11-19T10:00:01Z"}""")
    assert(EventTransforms.rejected(row).head().getAs[String]("reject_reason").contains("INVALID_ORDER_ID"))
  }
}
