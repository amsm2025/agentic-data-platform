import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("snowflake-smoke-test")
    .getOrCreate()
)

sf_options = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
}

df = (
    spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**sf_options)
    .option(
        "query",
        """
        SELECT
            CURRENT_USER() AS CURRENT_USER,
            CURRENT_ROLE() AS CURRENT_ROLE,
            CURRENT_DATABASE() AS CURRENT_DATABASE,
            CURRENT_SCHEMA() AS CURRENT_SCHEMA
        """
    )
    .load()
)

df.show(truncate=False)

spark.stop()