from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.postgres_sink import upsert_minute_metrics, upsert_product_metrics
from streaming.transforms import (
    build_order_aggregates,
    build_payment_aggregates,
    build_product_aggregates,
    parse_order_events,
    parse_payment_events,
)


def _kafka_stream(spark: SparkSession, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def _checkpoint(name: str) -> str:
    return f"{getenv('CHECKPOINT_DIR', '/checkpoints')}/{name}"


def main() -> None:
    database_url = getenv(
        "DATABASE_URL", "postgresql://streamforge:streamforge@postgres:5432/streamforge"
    )
    spark = (
        SparkSession.builder.appName("streamforge-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders, order_errors = parse_order_events(_kafka_stream(spark, "streamforge.orders.v1"))
    payments, payment_errors = parse_payment_events(
        _kafka_stream(spark, "streamforge.payments.v1")
    )
    errors = order_errors.unionByName(payment_errors)

    queries = [
        (
            build_order_aggregates(orders)
            .writeStream.outputMode("update")
            .foreachBatch(lambda frame, _: upsert_minute_metrics(frame.collect(), database_url))
            .option("checkpointLocation", _checkpoint("orders"))
            .start()
        ),
        (
            build_payment_aggregates(payments)
            .writeStream.outputMode("update")
            .foreachBatch(lambda frame, _: upsert_minute_metrics(frame.collect(), database_url))
            .option("checkpointLocation", _checkpoint("payments"))
            .start()
        ),
        (
            build_product_aggregates(orders)
            .writeStream.outputMode("update")
            .foreachBatch(lambda frame, _: upsert_product_metrics(frame.collect(), database_url))
            .option("checkpointLocation", _checkpoint("products"))
            .start()
        ),
        (
            errors.select(
                F.to_json(F.struct("source_topic", "raw_payload", "error_reason")).alias("value")
            )
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
            .option("topic", "streamforge.dead-letter.v1")
            .option("checkpointLocation", _checkpoint("dead-letter"))
            .start()
        ),
    ]
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
