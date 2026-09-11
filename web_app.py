from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="Agentic DataOps Platform",
    description="Interactive portfolio console for an end-to-end agentic data engineering platform.",
    version="1.1.0",
)


class TransactionRequest(BaseModel):
    order_id: str = Field(min_length=3, max_length=40)
    customer_id: str | None = None
    product_id: str | None = None
    quantity: int = 1
    amount: float = Field(gt=0)
    source_system: str = "ECOM"


class ReplayRequest(BaseModel):
    customer_id: str | None = None
    product_id: str | None = None
    quantity: int | None = None
    amount: float | None = None


class AgentRequest(BaseModel):
    question: str = Field(min_length=2, max_length=500)


transactions: list[dict] = [
    {
        "id": "evt-1001",
        "order_id": "ORD-1001",
        "customer_id": "CUST-101",
        "product_id": "PROD-21",
        "quantity": 2,
        "amount": 2500.0,
        "source_system": "ECOM",
        "status": "COMPLETE",
        "destination": "AGENTIC_DATA.ANALYTICS.FCT_SALES",
        "reason": None,
        "created_at": "2026-09-11T06:05:00Z",
        "timeline": ["RECEIVED", "KAFKA", "VALIDATED", "SNOWFLAKE", "DBT", "COMPLETE"],
    },
    {
        "id": "evt-1002",
        "order_id": "ORD-1002",
        "customer_id": "CUST-102",
        "product_id": "PROD-11",
        "quantity": 1,
        "amount": 1250.0,
        "source_system": "CRM",
        "status": "COMPLETE",
        "destination": "AGENTIC_DATA.ANALYTICS.FCT_SALES",
        "reason": None,
        "created_at": "2026-09-11T06:08:00Z",
        "timeline": ["RECEIVED", "KAFKA", "VALIDATED", "SNOWFLAKE", "DBT", "COMPLETE"],
    },
    {
        "id": "evt-1003",
        "order_id": "ORD-1003",
        "customer_id": None,
        "product_id": "PROD-08",
        "quantity": 3,
        "amount": 3600.0,
        "source_system": "ERP",
        "status": "DLQ",
        "destination": "orders.dlq",
        "reason": "CUSTOMER_ID_REQUIRED",
        "created_at": "2026-09-11T06:12:00Z",
        "timeline": ["RECEIVED", "KAFKA", "VALIDATION_FAILED", "DLQ"],
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_transaction(payload: TransactionRequest) -> str | None:
    if not payload.customer_id:
        return "CUSTOMER_ID_REQUIRED"
    if not payload.product_id:
        return "PRODUCT_ID_REQUIRED"
    if payload.quantity <= 0:
        return "INVALID_QUANTITY"
    return None


def pipeline_snapshot() -> list[dict]:
    return [
        {"name": "Python Producer", "detail": "transaction events", "state": "healthy"},
        {"name": "Kafka", "detail": "orders.raw", "state": "healthy"},
        {"name": "Spark", "detail": "validation + deduplication", "state": "healthy"},
        {"name": "Snowflake", "detail": "RAW.ORDERS", "state": "healthy"},
        {"name": "dbt", "detail": "STG_ORDERS → FCT_SALES", "state": "healthy"},
        {"name": "LangGraph Agent", "detail": "governed diagnostics", "state": "healthy"},
    ]


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Agentic DataOps Platform",
        "deployment": "Render portfolio demo",
        "version": app.version,
    }


@app.get("/api/dashboard")
def dashboard():
    complete = [t for t in transactions if t["status"] == "COMPLETE"]
    dlq = [t for t in transactions if t["status"] == "DLQ"]
    revenue = sum(float(t["amount"]) for t in complete)
    total = len(transactions)
    success_rate = round((len(complete) / total) * 100, 1) if total else 100.0
    return {
        "metrics": {
            "transactions": total,
            "processed": len(complete),
            "dlq": len(dlq),
            "success_rate": success_rate,
            "gross_sales": revenue,
            "pytest": "11 / 11",
            "dbt_tests": "5 / 5",
        },
        "pipeline": pipeline_snapshot(),
        "recent": list(reversed(transactions[-8:])),
    }


@app.get("/api/transactions")
def list_transactions():
    return list(reversed(transactions))


@app.post("/api/transactions")
def create_transaction(payload: TransactionRequest):
    if any(t["order_id"] == payload.order_id for t in transactions):
        raise HTTPException(status_code=409, detail="ORDER_ID_ALREADY_EXISTS")

    reason = validate_transaction(payload)
    failed = reason is not None
    item = {
        "id": f"evt-{uuid4().hex[:8]}",
        "order_id": payload.order_id,
        "customer_id": payload.customer_id,
        "product_id": payload.product_id,
        "quantity": payload.quantity,
        "amount": payload.amount,
        "source_system": payload.source_system.upper(),
        "status": "DLQ" if failed else "COMPLETE",
        "destination": "orders.dlq" if failed else "AGENTIC_DATA.ANALYTICS.FCT_SALES",
        "reason": reason,
        "created_at": now_iso(),
        "timeline": (
            ["RECEIVED", "KAFKA", "VALIDATION_FAILED", "DLQ"]
            if failed
            else ["RECEIVED", "KAFKA", "VALIDATED", "SNOWFLAKE", "DBT", "COMPLETE"]
        ),
    }
    transactions.append(item)
    return item


@app.get("/api/dlq")
def list_dlq():
    return list(reversed([t for t in transactions if t["status"] == "DLQ"]))


@app.post("/api/dlq/{event_id}/replay")
def replay_dlq(event_id: str, patch: ReplayRequest):
    item = next((t for t in transactions if t["id"] == event_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="DLQ_EVENT_NOT_FOUND")
    if item["status"] != "DLQ":
        raise HTTPException(status_code=409, detail="EVENT_IS_NOT_IN_DLQ")

    candidate = TransactionRequest(
        order_id=item["order_id"],
        customer_id=patch.customer_id if patch.customer_id is not None else item["customer_id"],
        product_id=patch.product_id if patch.product_id is not None else item["product_id"],
        quantity=patch.quantity if patch.quantity is not None else item["quantity"],
        amount=patch.amount if patch.amount is not None else item["amount"],
        source_system=item["source_system"],
    )
    reason = validate_transaction(candidate)
    if reason:
        raise HTTPException(status_code=422, detail=reason)

    item.update(
        customer_id=candidate.customer_id,
        product_id=candidate.product_id,
        quantity=candidate.quantity,
        amount=candidate.amount,
        status="COMPLETE",
        destination="AGENTIC_DATA.ANALYTICS.FCT_SALES",
        reason=None,
        timeline=["DLQ", "REPLAY", "KAFKA", "VALIDATED", "SNOWFLAKE", "DBT", "COMPLETE"],
    )
    return item


@app.post("/api/agent")
def ask_agent(payload: AgentRequest):
    q = payload.question.strip()
    lower = q.lower()

    destructive = ("delete ", "drop ", "truncate ", "update ", "insert ")
    if any(token in lower for token in destructive):
        return {
            "answer": "Request blocked. The DataOps agent only permits governed read-only diagnostics.",
            "tool": "run_readonly_sql",
            "status": "blocked",
            "evidence": "Destructive or mutating SQL is outside the agent policy boundary.",
        }

    complete = [t for t in transactions if t["status"] == "COMPLETE"]
    dlq = [t for t in transactions if t["status"] == "DLQ"]
    if "dlq" in lower or "failed" in lower or "rejected" in lower:
        return {
            "answer": f"There are currently {len(dlq)} transaction(s) in the demo DLQ.",
            "tool": "inspect_dlq",
            "status": "ok",
            "evidence": [
                {"order_id": t["order_id"], "reason": t["reason"], "topic": "orders.dlq"}
                for t in dlq[:5]
            ],
        }
    if "revenue" in lower or "sales" in lower:
        gross = sum(float(t["amount"]) for t in complete)
        return {
            "answer": f"The demo analytics layer currently contains {len(complete)} completed order(s) with gross sales of ₱{gross:,.2f}.",
            "tool": "run_readonly_sql",
            "status": "ok",
            "evidence": "Portfolio simulation of AGENTIC_DATA.ANALYTICS.FCT_SALES.",
        }
    if "row" in lower or "fct_sales" in lower or "how many" in lower:
        return {
            "answer": f"FCT_SALES currently represents {len(complete)} completed order(s) in this interactive showcase.",
            "tool": "run_readonly_sql",
            "status": "ok",
            "evidence": "SELECT COUNT(*) FROM AGENTIC_DATA.ANALYTICS.FCT_SALES;",
        }
    if "pipeline" in lower or "health" in lower:
        return {
            "answer": "All showcase pipeline stages are healthy: producer, Kafka, Spark, Snowflake, dbt, and the governed agent.",
            "tool": "get_pipeline_contract",
            "status": "ok",
            "evidence": pipeline_snapshot(),
        }
    return {
        "answer": "I can inspect pipeline health, explain DLQ failures, summarize sales, count FCT_SALES rows, and demonstrate read-only SQL guardrails.",
        "tool": "get_pipeline_contract",
        "status": "ok",
        "evidence": "The full project uses LangGraph tools for Snowflake, pipeline-contract, and DLQ diagnostics.",
    }


@app.get("/", response_class=HTMLResponse)
def home():
    return HTML_PAGE


HTML_PAGE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Agentic DataOps Operations Console</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#eaf2ff;background:#06101d;--panel:#0b1828;--panel2:#0f2033;--line:#203651;--muted:#93a9c3;--cyan:#62e6ef;--green:#55d98c;--amber:#f7c65b;--red:#ff7385}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top right,#102a40 0,#06101d 42%);min-height:100vh}.shell{display:grid;grid-template-columns:240px 1fr;min-height:100vh}.sidebar{border-right:1px solid var(--line);padding:24px 18px;background:#071321;position:sticky;top:0;height:100vh}.brand{font-weight:900;letter-spacing:-.03em;font-size:1.2rem}.brand small{display:block;color:var(--cyan);font-size:.68rem;letter-spacing:.16em;text-transform:uppercase;margin-top:5px}.nav{display:grid;gap:7px;margin-top:32px}.nav button{border:0;background:transparent;color:#a9bdd4;text-align:left;padding:11px 12px;border-radius:10px;font-weight:700;cursor:pointer}.nav button.active,.nav button:hover{background:#10243a;color:#fff}.side-note{position:absolute;bottom:20px;color:#667f9b;font-size:.78rem;line-height:1.5}.main{padding:28px 30px 60px;max-width:1500px;width:100%}.topbar{display:flex;justify-content:space-between;gap:20px;align-items:center}.eyebrow{color:var(--cyan);font-weight:900;letter-spacing:.13em;text-transform:uppercase;font-size:.72rem}.topbar h1{margin:4px 0 0;font-size:clamp(1.9rem,3vw,3rem);letter-spacing:-.045em}.health{border:1px solid #244f43;background:#0d2a25;color:#91efb5;padding:8px 12px;border-radius:999px;font-weight:800;font-size:.8rem}.page{display:none;margin-top:26px}.page.active{display:block}.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:13px}.metric,.card{background:linear-gradient(180deg,#0e2033,#091726);border:1px solid var(--line);border-radius:16px}.metric{padding:18px}.metric label{display:block;color:var(--muted);font-size:.78rem}.metric strong{display:block;margin-top:6px;font-size:1.65rem}.grid2{display:grid;grid-template-columns:1.25fr .75fr;gap:16px;margin-top:16px}.card{padding:18px}.card h2{font-size:1rem;margin:0 0 14px}.pipeline{display:grid;grid-template-columns:repeat(6,minmax(115px,1fr));gap:10px;overflow:auto}.stage{min-width:115px;border:1px solid #29415f;background:#0a1828;padding:13px;border-radius:12px;position:relative}.stage:after{content:'→';position:absolute;right:-12px;top:28px;color:#52708f}.stage:last-child:after{display:none}.stage b{display:block;font-size:.82rem}.stage span{color:var(--muted);font-size:.72rem}.dot{width:7px;height:7px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;font-size:.84rem}th,td{text-align:left;padding:11px 8px;border-bottom:1px solid #182e46}th{color:#7f98b4;font-size:.72rem;text-transform:uppercase;letter-spacing:.06em}.pill{display:inline-block;padding:5px 8px;border-radius:999px;font-size:.7rem;font-weight:900}.ok{color:#86efac;background:#153426}.bad{color:#ff9baa;background:#401b25}.form-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}input,select,textarea{width:100%;background:#071422;border:1px solid #2b425f;color:#edf5ff;border-radius:10px;padding:11px 12px;outline:none}input:focus,textarea:focus{border-color:#56cbd5}.btn{border:0;border-radius:10px;padding:11px 15px;font-weight:900;cursor:pointer;background:var(--cyan);color:#031016}.btn.secondary{background:#14283e;color:#d8e7f8;border:1px solid #2d4765}.btn.danger{background:#44202a;color:#ff9ead}.actions{display:flex;gap:9px;flex-wrap:wrap;margin-top:14px}.timeline{display:flex;gap:7px;align-items:center;flex-wrap:wrap}.timeline span{padding:5px 8px;border-radius:7px;background:#10233a;color:#a9cae7;font-size:.68rem;font-weight:800}.timeline i{font-style:normal;color:#4e6a88}.terminal{background:#03080d;border:1px solid #1a3046;color:#b9f4ff;border-radius:12px;padding:14px;font:12px/1.65 Consolas,monospace;white-space:pre-wrap;min-height:150px}.chat{display:grid;grid-template-rows:1fr auto;min-height:430px}.messages{display:flex;flex-direction:column;gap:10px;overflow:auto;padding-bottom:15px}.bubble{max-width:82%;padding:12px 14px;border-radius:14px;line-height:1.5;font-size:.87rem}.bubble.user{align-self:flex-end;background:#183b59}.bubble.agent{align-self:flex-start;background:#10243a;border:1px solid #27415f}.tool{color:#72e5ee;font-size:.71rem;font-weight:900;margin-top:8px}.agent-input{display:grid;grid-template-columns:1fr auto;gap:10px}.guardrails{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.guard{padding:12px;border:1px solid #25405d;border-radius:10px;background:#0a1928}.guard strong{display:block}.guard small{color:var(--muted)}.subtle{color:var(--muted);font-size:.82rem;line-height:1.55}.section-title{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:14px}.section-title h2{margin:0}.section-title p{margin:3px 0 0;color:var(--muted);font-size:.8rem}@media(max-width:1050px){.shell{grid-template-columns:1fr}.sidebar{position:relative;height:auto;border-right:0;border-bottom:1px solid var(--line)}.nav{display:flex;overflow:auto;margin-top:18px}.side-note{display:none}.metrics{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}.pipeline{grid-template-columns:repeat(6,170px)}}@media(max-width:650px){.main{padding:20px 14px 40px}.metrics,.form-grid,.guardrails{grid-template-columns:1fr}.topbar{align-items:flex-start}.health{white-space:nowrap}}
</style>
</head>
<body>
<div class="shell">
<aside class="sidebar"><div class="brand">Agentic DataOps<small>Operations Console</small></div><nav class="nav">
<button class="active" data-page="dashboard">Dashboard</button><button data-page="transactions">Transactions</button><button data-page="dlq">DLQ & Replay</button><button data-page="agent">AI DataOps Agent</button><button data-page="governance">Governance</button>
</nav><div class="side-note">Portfolio showcase<br>Kafka • Spark • Snowflake • dbt • LangGraph</div></aside>
<main class="main"><header class="topbar"><div><div class="eyebrow">Real-time data engineering showcase</div><h1>Agentic DataOps Platform</h1></div><div class="health">● PLATFORM HEALTHY</div></header>

<section id="dashboard" class="page active"><div class="metrics" id="metrics"></div><div class="grid2"><div class="card"><h2>Live Pipeline Overview</h2><div class="pipeline" id="pipeline"></div></div><div class="card"><h2>Quality Gate</h2><div class="guardrails"><div class="guard"><strong>11 / 11 ✓</strong><small>pytest</small></div><div class="guard"><strong>5 / 5 ✓</strong><small>dbt tests</small></div><div class="guard"><strong>Read-only ✓</strong><small>SQL policy</small></div></div></div></div><div class="card" style="margin-top:16px"><h2>Recent Transactions</h2><div class="table-wrap"><table><thead><tr><th>Order</th><th>Source</th><th>Amount</th><th>Status</th><th>Destination</th></tr></thead><tbody id="recentRows"></tbody></table></div></div></section>

<section id="transactions" class="page"><div class="section-title"><div><h2>Transaction Simulator</h2><p>Publish a valid or intentionally invalid order and watch its governed lifecycle.</p></div></div><div class="card"><div class="form-grid"><input id="orderId" placeholder="Order ID" value="ORD-2001"><input id="customerId" placeholder="Customer ID" value="CUST-501"><input id="productId" placeholder="Product ID" value="PROD-99"><input id="quantity" type="number" placeholder="Quantity" value="2"><input id="amount" type="number" step="0.01" placeholder="Amount" value="2750"><select id="sourceSystem"><option>ECOM</option><option>ERP</option><option>CRM</option></select></div><div class="actions"><button class="btn" onclick="sendTransaction(false)">Send Valid Transaction</button><button class="btn secondary" onclick="sendTransaction(true)">Generate Invalid Transaction</button></div></div><div class="grid2"><div class="card"><h2>Transaction Lifecycle</h2><div id="transactionResult" class="terminal">Ready. Submit a transaction to visualize the flow.</div></div><div class="card"><h2>What this demonstrates</h2><p class="subtle">The public UI simulates the same architecture used by the full project: producer → Kafka <b>orders.raw</b> → Spark validation/deduplication → trusted Snowflake path or <b>orders.dlq</b> → dbt analytics.</p></div></div><div class="card" style="margin-top:16px"><h2>Transaction Monitor</h2><div class="table-wrap"><table><thead><tr><th>Order</th><th>Customer</th><th>Product</th><th>Amount</th><th>Status</th><th>Lifecycle</th></tr></thead><tbody id="transactionRows"></tbody></table></div></div></section>

<section id="dlq" class="page"><div class="section-title"><div><h2>Dead Letter Queue</h2><p>Inspect validation failures, correct bad data, and replay the event.</p></div></div><div id="dlqCards"></div></section>

<section id="agent" class="page"><div class="grid2"><div class="card chat"><div><h2>Ask the DataOps Agent</h2><div class="messages" id="messages"><div class="bubble agent">Ask about FCT_SALES rows, revenue, DLQ failures, pipeline health, or try a destructive SQL request.<div class="tool">GOVERNED AGENT</div></div></div></div><div class="agent-input"><input id="agentQuestion" placeholder="How many rows are in FCT_SALES?" onkeydown="if(event.key==='Enter') askAgent()"><button class="btn" onclick="askAgent()">Ask</button></div></div><div class="card"><h2>Agent Tool Boundary</h2><div class="terminal">LangGraph DataOps Agent
  ├─ run_readonly_sql
  ├─ inspect_dlq
  └─ get_pipeline_contract

Allowed: diagnostic reads
Blocked: INSERT / UPDATE / DELETE / DROP / TRUNCATE

Full project target:
AGENTIC_DATA.ANALYTICS.FCT_SALES</div><div class="actions"><button class="btn secondary" onclick="quickAsk('How many rows are in FCT_SALES?')">Count rows</button><button class="btn secondary" onclick="quickAsk('Why are records in the DLQ?')">Inspect DLQ</button><button class="btn danger" onclick="quickAsk('DELETE FROM FCT_SALES')">Test guardrail</button></div></div></div></section>

<section id="governance" class="page"><div class="card"><h2>Governance & Safety Controls</h2><div class="guardrails"><div class="guard"><strong>SELECT</strong><small>Allowed</small></div><div class="guard"><strong>Diagnostic tools</strong><small>Allowed</small></div><div class="guard"><strong>Human remediation</strong><small>Required for changes</small></div><div class="guard"><strong>INSERT / UPDATE</strong><small>Blocked</small></div><div class="guard"><strong>DELETE / TRUNCATE</strong><small>Blocked</small></div><div class="guard"><strong>DROP</strong><small>Blocked</small></div></div></div><div class="card" style="margin-top:16px"><h2>Reference Architecture</h2><div class="terminal">Python Producer → Kafka orders.raw → Spark Structured Streaming
                                      ├─ VALID   → Snowflake RAW.ORDERS → dbt → STG_ORDERS → FCT_SALES
                                      └─ INVALID → Kafka orders.dlq

LangGraph DataOps Agent → governed tools → read-only Snowflake SQL / DLQ inspection / pipeline contract</div></div></section>
</main></div>
<script>
const peso=v=>'₱'+Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
async function api(path,options={}){const r=await fetch(path,{headers:{'Content-Type':'application/json'},...options});const data=await r.json();if(!r.ok)throw new Error(data.detail||'Request failed');return data}
function statusPill(s){return `<span class="pill ${s==='COMPLETE'?'ok':'bad'}">${s}</span>`}
function renderTimeline(items){return `<div class="timeline">${items.map((x,i)=>`${i?'<i>→</i>':''}<span>${x}</span>`).join('')}</div>`}
async function refresh(){const d=await api('/api/dashboard');document.getElementById('metrics').innerHTML=[['Transactions',d.metrics.transactions],['Processed',d.metrics.processed],['DLQ',d.metrics.dlq],['Success Rate',d.metrics.success_rate+'%']].map(x=>`<div class="metric"><label>${x[0]}</label><strong>${x[1]}</strong></div>`).join('');document.getElementById('pipeline').innerHTML=d.pipeline.map(x=>`<div class="stage"><b><i class="dot"></i>${x.name}</b><span>${x.detail}</span></div>`).join('');document.getElementById('recentRows').innerHTML=d.recent.map(t=>`<tr><td>${t.order_id}</td><td>${t.source_system}</td><td>${peso(t.amount)}</td><td>${statusPill(t.status)}</td><td>${t.destination}</td></tr>`).join('');const ts=await api('/api/transactions');document.getElementById('transactionRows').innerHTML=ts.map(t=>`<tr><td>${t.order_id}</td><td>${t.customer_id||'—'}</td><td>${t.product_id||'—'}</td><td>${peso(t.amount)}</td><td>${statusPill(t.status)}</td><td>${renderTimeline(t.timeline)}</td></tr>`).join('');await refreshDlq()}
async function sendTransaction(invalid){let body={order_id:document.getElementById('orderId').value,customer_id:invalid?null:document.getElementById('customerId').value,product_id:document.getElementById('productId').value,quantity:Number(document.getElementById('quantity').value),amount:Number(document.getElementById('amount').value),source_system:document.getElementById('sourceSystem').value};try{const t=await api('/api/transactions',{method:'POST',body:JSON.stringify(body)});document.getElementById('transactionResult').textContent=`${t.order_id}\n\n${t.timeline.join('  →  ')}\n\nStatus: ${t.status}\nDestination: ${t.destination}${t.reason?'\nReason: '+t.reason:''}`;document.getElementById('orderId').value='ORD-'+Math.floor(2000+Math.random()*7000);await refresh()}catch(e){document.getElementById('transactionResult').textContent='ERROR: '+e.message}}
async function refreshDlq(){const list=await api('/api/dlq');document.getElementById('dlqCards').innerHTML=list.length?list.map(t=>`<div class="card" style="margin-bottom:12px"><div class="section-title"><div><h2>${t.order_id}</h2><p>${t.reason} • ${t.created_at}</p></div>${statusPill(t.status)}</div>${renderTimeline(t.timeline)}<div class="form-grid" style="margin-top:14px"><input id="fix-${t.id}" placeholder="Customer ID" value="${t.customer_id||'CUST-FIXED'}"><input value="${t.product_id||''}" disabled><input value="${t.quantity}" disabled></div><div class="actions"><button class="btn" onclick="replay('${t.id}')">Fix & Replay</button></div></div>`).join(''):'<div class="card"><p class="subtle">DLQ is empty. Generate an invalid transaction to demonstrate exception handling.</p></div>'}
async function replay(id){try{await api(`/api/dlq/${id}/replay`,{method:'POST',body:JSON.stringify({customer_id:document.getElementById('fix-'+id).value})});await refresh()}catch(e){alert(e.message)}}
async function askAgent(){const input=document.getElementById('agentQuestion');const q=input.value.trim();if(!q)return;const box=document.getElementById('messages');box.innerHTML+=`<div class="bubble user">${q}</div>`;input.value='';try{const r=await api('/api/agent',{method:'POST',body:JSON.stringify({question:q})});box.innerHTML+=`<div class="bubble agent">${r.answer}<div class="tool">${r.tool.toUpperCase()} • ${r.status.toUpperCase()}</div></div>`;box.scrollTop=box.scrollHeight}catch(e){box.innerHTML+=`<div class="bubble agent">${e.message}</div>`}}
function quickAsk(q){document.getElementById('agentQuestion').value=q;askAgent()}
document.querySelectorAll('.nav button').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.nav button').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));b.classList.add('active');document.getElementById(b.dataset.page).classList.add('active')}));
refresh();
</script>
</body></html>'''
