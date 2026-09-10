import os
from typing import Annotated, TypedDict

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

from .tools import get_pipeline_contract, inspect_dlq, run_readonly_sql

load_dotenv()


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]


tools = [run_readonly_sql, get_pipeline_contract, inspect_dlq]
model = ChatOpenAI(
    model=os.getenv("OPENAI_MODEL", "gpt-5.6"),
    temperature=0,
    reasoning_effort="none",
).bind_tools(tools)


def assistant_node(state: AgentState):
    system_context = (
    "You are a governed DataOps agent. Diagnose data pipeline and analytics issues. "
    "Use tools when needed. Never invent query results. Only read-only SQL is available. "
    "For changes, explain a proposed remediation and require human approval outside the agent. "
    "The Snowflake database is AGENTIC_DATA. "
    "Raw streaming orders are stored in AGENTIC_DATA.RAW.ORDERS. "
    "Analytics models are stored in AGENTIC_DATA.ANALYTICS. "
    "The sales mart is AGENTIC_DATA.ANALYTICS.FCT_SALES. "
    "The RAW.ORDERS table contains order-level fields such as ORDER_ID, EVENT_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, AMOUNT, DQ_STATUS, and DQ_REASON. "
    "The FCT_SALES mart is aggregated and contains ORDER_DATE, SOURCE_SYSTEM, ORDER_COUNT, UNITS_SOLD, and GROSS_SALES; do not query order-level columns from FCT_SALES. "
    "When answering sales questions, query the fully qualified sales mart table."
)
    return {"messages": [model.invoke([( "system", system_context), *state["messages"]])]}


builder = StateGraph(AgentState)
builder.add_node("assistant", assistant_node)
builder.add_node("tools", ToolNode(tools))
builder.set_entry_point("assistant")
builder.add_conditional_edges("assistant", tools_condition, {"tools": "tools", END: END})
builder.add_edge("tools", "assistant")
graph = builder.compile()


if __name__ == "__main__":
    question = input("Ask the DataOps agent: ")
    result = graph.invoke({"messages": [HumanMessage(content=question)]})
    print(result["messages"][-1].content)
