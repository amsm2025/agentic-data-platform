import os

import pytest

from agent.tools import run_readonly_sql


@pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_ACCOUNT"),
    reason="Snowflake environment variables are not configured",
)
def test_sales_mart_is_readable():
    result = run_readonly_sql.invoke(
        {
            "sql": (
                "SELECT COUNT(*) AS ROW_COUNT "
                "FROM AGENTIC_DATA.ANALYTICS.FCT_SALES"
            )
        }
    )

    assert result
    assert result[0]["ROW_COUNT"] >= 1