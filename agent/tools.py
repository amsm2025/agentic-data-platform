import os
import re
import json
from kafka import KafkaConsumer
from typing import Any

import snowflake.connector
from langchain_core.tools import tool

READ_ONLY = re.compile(r"^\s*(select|with|show|describe|desc)\b", re.IGNORECASE)
FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|alter|truncate|grant|revoke|create|replace)\b", re.IGNORECASE)


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        role=os.environ["SNOWFLAKE_ROLE"],
    )


@tool
def run_readonly_sql(sql: str) -> list[dict[str, Any]]:
    """Execute a read-only Snowflake SQL query. Mutating statements are rejected."""
    statement = sql.strip().rstrip(";")
    if not READ_ONLY.search(statement) or FORBIDDEN.search(statement):
        raise ValueError("Only read-only SQL is allowed")
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(statement)
            columns = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchmany(100)
            return [dict(zip(columns, row)) for row in rows]

@tool
def inspect_dlq(max_messages: int = 20) -> list[dict[str, Any]]:
    """Read recent records from the Kafka dead-letter topic without modifying them."""

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic = os.getenv(
        "KAFKA_DLQ_TOPIC",
        "orders.dlq",
    )

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    records = []

    try:
        for message in consumer:
            records.append(
                {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "value": message.value,
                }
            )

            if len(records) >= max_messages:
                break
    finally:
        consumer.close()

    return records


@tool
def get_pipeline_contract() -> str:
    """Return the logical contract for the demo order pipeline."""
    return (
    "The orders.raw pipeline requires event_id, event_ts, order_id, customer_id, "
    "product_id, quantity > 0, amount >= 0, source_system, and schema_version. "
    "event_id must be unique after streaming deduplication. "
    "Valid records are written to AGENTIC_DATA.RAW.ORDERS. "
    "Invalid records are quarantined to the Kafka topic orders.dlq and are not written "
    "to AGENTIC_DATA.RAW.ORDERS. Therefore, absence of INVALID records in RAW.ORDERS "
    "does not prove that the DLQ is empty."
)
