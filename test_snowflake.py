import os

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    role=os.environ["SNOWFLAKE_ROLE"],
)

cur = conn.cursor()

try:
    cur.execute(
        """
        SELECT
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_WAREHOUSE(),
            CURRENT_DATABASE(),
            CURRENT_SCHEMA()
        """
    )

    print(cur.fetchone())

    cur.execute("SELECT COUNT(*) FROM ORDERS")
    print("ORDERS row count:", cur.fetchone()[0])

finally:
    cur.close()
    conn.close()