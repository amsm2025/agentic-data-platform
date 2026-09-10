import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    current_timestamp,
    from_json,
    lit,
    to_json,
    to_timestamp,
    struct,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders.raw")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders.dlq")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", "/tmp/agentic-orders-checkpoint")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

sf_options = {
    "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLAKE_PASSWORD,
    "sfWarehouse": SNOWFLAKE_WAREHOUSE,
    "sfDatabase": SNOWFLAKE_DATABASE,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfRole": SNOWFLAKE_ROLE,
}

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source_system", StringType(), True),
    StructField("schema_version", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("agentic-order-stream")
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_payload"),
    )
    .withColumn("e", from_json(col("raw_payload"), schema))
    .select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "raw_payload",
        "e.*",
    )
    .withColumn("event_ts", to_timestamp("event_ts"))
    .withColumn(
        "expected_amount",
        col("quantity") * col("unit_price"),
    )
    .withColumn(
        "amount_variance",
        spark_abs(col("amount") - col("expected_amount")),
    )
)

classified = (
    parsed
    .withColumn(
        "dq_reason",
        when(col("event_id").isNull(), lit("missing_event_id"))
        .when(col("event_ts").isNull(), lit("invalid_event_ts"))
        .when(col("order_id").isNull(), lit("missing_order_id"))
        .when(col("customer_id").isNull(), lit("missing_customer_id"))
        .when(col("product_id").isNull(), lit("missing_product_id"))
        .when(col("source_system").isNull(), lit("missing_source_system"))
        .when(col("schema_version") != 1, lit("unsupported_schema_version"))
        .when(col("quantity").isNull(), lit("missing_quantity"))
        .when(col("quantity") <= 0, lit("invalid_quantity"))
        .when(col("unit_price").isNull(), lit("missing_unit_price"))
        .when(col("unit_price") < 0, lit("invalid_unit_price"))
        .when(col("amount").isNull(), lit("missing_amount"))
        .when(col("amount") < 0, lit("invalid_amount"))
        .when(col("amount_variance") > 0.01, lit("amount_mismatch"))
        .otherwise(lit(None)),
    )
    .withColumn(
        "dq_status",
        when(col("dq_reason").isNull(), lit("VALID"))
        .otherwise(lit("INVALID")),
    )
)

valid = (
    classified
    .filter(col("dq_status") == "VALID")
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id"])
    .withColumn("processed_at", current_timestamp())
)

invalid = (
    classified
    .filter(col("dq_status") == "INVALID")
    .withColumn("processed_at", current_timestamp())
)

def write_valid_to_snowflake(batch_df, batch_id):
    (
        batch_df
        .select(
            "event_id",
            "event_type",
            "event_ts",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "amount",
            "source_system",
            "schema_version",
            "expected_amount",
            "amount_variance",
            "dq_status",
            "dq_reason",
            "processed_at",
        )
        .write
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", "ORDERS")
        .mode("append")
        .save()
    )

valid_query = (
    valid.writeStream
    .foreachBatch(write_valid_to_snowflake)
    .option("checkpointLocation", f"{CHECKPOINT}/valid-snowflake")
    .start()
)

dlq_payload = (
    invalid
    .select(
        to_json(
            struct(
                col("raw_payload"),
                col("dq_reason"),
                col("dq_status"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("processed_at"),
            )
        ).alias("value")
    )
)

dlq_query = (
    dlq_payload.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", DLQ_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT}/dlq")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()