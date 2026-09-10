from agent.tools import inspect_dlq


def test_inspect_dlq_tool_exists():
    assert inspect_dlq.name == "inspect_dlq"


def test_inspect_dlq_has_description():
    assert inspect_dlq.description
    assert "dead-letter" in inspect_dlq.description.lower()