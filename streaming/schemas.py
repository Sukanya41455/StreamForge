from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ORDER_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("region", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)

PAYMENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("event_time", StringType(), True),
    ]
)
