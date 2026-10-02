from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from streaming.schemas import ORDER_SCHEMA, PAYMENT_SCHEMA
from streaming.validation import validate_order_record, validate_payment_record


def _parse_events(raw_stream: DataFrame, schema, validator) -> tuple[DataFrame, DataFrame]:
    validate = F.udf(validator, StringType())
    parsed = (
        raw_stream.select(
            F.col("topic").alias("source_topic"),
            F.col("value").cast("string").alias("raw_payload"),
        )
        .withColumn("event", F.from_json("raw_payload", schema))
        .withColumn("error_reason", validate("event"))
    )
    valid = (
        parsed.filter(F.col("error_reason").isNull())
        .select("event.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )
    errors = parsed.filter(F.col("error_reason").isNotNull()).select(
        "source_topic", "raw_payload", "error_reason"
    )
    return valid, errors


def parse_order_events(raw_stream: DataFrame) -> tuple[DataFrame, DataFrame]:
    return _parse_events(raw_stream, ORDER_SCHEMA, validate_order_record)


def parse_payment_events(raw_stream: DataFrame) -> tuple[DataFrame, DataFrame]:
    return _parse_events(raw_stream, PAYMENT_SCHEMA, validate_payment_record)


def build_order_aggregates(orders: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    return (
        orders.withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
        .groupBy(F.window("event_time", "1 minute"))
        .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
        .select(F.col("window.start").alias("window_start"), "orders", "revenue")
    )


def build_payment_aggregates(payments: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    return (
        payments.withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
        .groupBy(F.window("event_time", "1 minute"))
        .agg(
            F.count("*").alias("payments"),
            (
                F.sum(F.when(F.col("status") == "succeeded", F.lit(1)).otherwise(F.lit(0)))
                / F.count("*")
                * F.lit(100)
            ).alias("payment_success_rate"),
        )
        .select("window.start", "payments", "payment_success_rate")
        .withColumnRenamed("start", "window_start")
    )


def build_product_aggregates(orders: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    return (
        orders.withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
        .groupBy(F.window("event_time", "5 minutes"), "product_id")
        .agg(F.count("*").alias("order_count"), F.sum("amount").alias("revenue"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "order_count",
            "revenue",
        )
    )
