import pytest
from agent.tools import FORBIDDEN, READ_ONLY

@pytest.mark.parametrize("sql", [
    "select * from fct_sales",
    "with x as (select 1) select * from x",
    "show tables",
    "describe table fct_sales",
])
def test_readonly_statements_are_detected(sql):
    assert READ_ONLY.search(sql)
    assert not FORBIDDEN.search(sql)

@pytest.mark.parametrize("sql", [
    "delete from fct_sales",
    "drop table fct_sales",
    "update fct_sales set gross_sales = 0",
    "create table x as select 1",
])
def test_mutating_statements_are_forbidden(sql):
    assert FORBIDDEN.search(sql)
