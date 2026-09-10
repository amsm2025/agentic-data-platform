from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Agentic DataOps Platform",
    description="Portfolio demo for an end-to-end agentic data engineering platform.",
    version="1.0.0",
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Agentic DataOps Platform",
        "deployment": "Render portfolio demo",
    }


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Agentic DataOps Platform</title>
  <style>
    :root {
      color-scheme: dark;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: #07111f;
      color: #e8eef7;
    }
    .wrap {
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
      padding: 64px 0 80px;
    }
    .eyebrow {
      color: #67e8f9;
      font-size: 0.82rem;
      font-weight: 800;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }
    h1 {
      max-width: 900px;
      margin: 14px 0 18px;
      font-size: clamp(2.5rem, 7vw, 5.6rem);
      line-height: 0.96;
      letter-spacing: -0.055em;
    }
    .lead {
      max-width: 820px;
      color: #a9b9cf;
      font-size: 1.13rem;
      line-height: 1.75;
    }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 9px;
      margin: 28px 0 0;
    }
    .badge {
      border: 1px solid #24364d;
      background: #0c1a2b;
      border-radius: 999px;
      padding: 8px 12px;
      color: #d4dfed;
      font-size: 0.88rem;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
      margin-top: 42px;
    }
    .card {
      border: 1px solid #22334a;
      border-radius: 18px;
      background: linear-gradient(180deg, #0d1b2d 0%, #091522 100%);
      padding: 22px;
    }
    .card h2 {
      margin: 0 0 10px;
      font-size: 1.05rem;
    }
    .card p {
      margin: 0;
      color: #9eb0c8;
      line-height: 1.62;
    }
    .flow {
      margin-top: 18px;
      border-radius: 14px;
      background: #050b13;
      border: 1px solid #18283b;
      padding: 18px;
      overflow-x: auto;
      color: #c8f7ff;
      font: 0.86rem/1.6 Consolas, Monaco, monospace;
      white-space: pre;
    }
    .proof {
      margin-top: 18px;
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }
    .metric {
      padding: 16px;
      border-radius: 14px;
      border: 1px solid #22334a;
      background: #0a1727;
    }
    .metric strong {
      display: block;
      font-size: 1.35rem;
      color: #f2f7fb;
    }
    .metric span { color: #8fa3bd; font-size: 0.86rem; }
    .actions {
      margin-top: 36px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }
    a.button {
      display: inline-block;
      text-decoration: none;
      border-radius: 12px;
      padding: 12px 16px;
      font-weight: 750;
      background: #67e8f9;
      color: #041018;
    }
    a.secondary {
      background: transparent;
      border: 1px solid #2c415a;
      color: #dbe7f5;
    }
    .note {
      margin-top: 28px;
      color: #758aa6;
      font-size: 0.9rem;
      line-height: 1.6;
    }
    @media (max-width: 760px) {
      .grid, .proof { grid-template-columns: 1fr; }
      .wrap { padding-top: 44px; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <div class="eyebrow">Senior Agentic Data Engineering Portfolio</div>
    <h1>Agentic DataOps Platform</h1>
    <p class="lead">
      A governed data platform that streams enterprise order events through Kafka and Spark,
      persists trusted records in Snowflake, transforms analytics with dbt, and exposes
      diagnostic workflows through a LangGraph-powered DataOps agent.
    </p>

    <div class="badges">
      <span class="badge">LangGraph</span>
      <span class="badge">OpenAI</span>
      <span class="badge">Kafka</span>
      <span class="badge">Spark Structured Streaming</span>
      <span class="badge">Snowflake</span>
      <span class="badge">dbt</span>
      <span class="badge">Terraform</span>
      <span class="badge">Docker</span>
      <span class="badge">Python</span>
    </div>

    <section class="grid">
      <article class="card">
        <h2>Streaming + Data Quality</h2>
        <p>Order events are validated, deduplicated, enriched, and separated into trusted and quarantined paths.</p>
      </article>
      <article class="card">
        <h2>Governed Analytics</h2>
        <p>Valid records land in Snowflake and are transformed into tested analytics models with dbt.</p>
      </article>
      <article class="card">
        <h2>Agentic DataOps</h2>
        <p>LangGraph orchestrates governed tools for read-only SQL, pipeline contract inspection, and DLQ diagnostics.</p>
      </article>
    </section>

    <div class="flow">Python Producer
      |
      v
Kafka orders.raw
      |
      v
Spark Structured Streaming
  |                 |
  | VALID           | INVALID
  v                 v
Snowflake          Kafka DLQ
RAW.ORDERS         orders.dlq
  |
  v
dbt STG_ORDERS -> FCT_SALES
  |
  v
LangGraph DataOps Agent</div>

    <section class="proof">
      <div class="metric"><strong>11 / 11</strong><span>Python tests passing</span></div>
      <div class="metric"><strong>5 / 5</strong><span>dbt tests passing</span></div>
      <div class="metric"><strong>Read-only</strong><span>destructive SQL blocked</span></div>
    </section>

    <div class="actions">
      <a class="button" href="https://github.com/amsm2025/agentic-data-platform" target="_blank" rel="noreferrer">View GitHub Repository</a>
      <a class="button secondary" href="/health">Service Health</a>
    </div>

    <p class="note">
      This Render deployment is the public portfolio showcase for the project. The full data platform includes
      Kafka, Spark Structured Streaming, Snowflake, dbt, Terraform, and the governed LangGraph agent workflow.
    </p>
  </main>
</body>
</html>
"""
