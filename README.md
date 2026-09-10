# Agentic DataOps Platform

A portfolio-grade Senior Agentic Data Engineer project that combines **LangGraph, Apache Kafka, Apache Spark, dbt, Snowflake, and Terraform** into one end-to-end data platform.

## Business scenario
A multi-system enterprise receives customer, order, inventory, HR, and CRM events from upstream applications. The platform must ingest events in near real time, validate and transform them, load trusted data into Snowflake, build analytics models with dbt, and let an AI agent diagnose pipeline issues and answer natural-language data questions safely.

## Architecture

```text
ERP / CRM / HRIS / Product APIs
          |
          v
 Python Event Producers / API Connectors
          |
          v
       Apache Kafka
          |
          v
 Spark Structured Streaming
  - schema validation
  - deduplication
  - enrichment
  - anomaly routing
          |
          +------------------> quarantine / DLQ
          |
          v
      Snowflake RAW
          |
          v
          dbt
  STAGING -> CORE -> MARTS
          |
          v
   BI / Analytics / Metrics
          ^
          |
  LangGraph DataOps Agent
  - inspect pipeline health
  - query metadata
  - generate safe SQL
  - propose dbt models/tests
  - explain anomalies
  - require approval for writes
```

## Why each technology is here

- **LangGraph**: stateful agent orchestration with explicit tools and approval gates.
- **Kafka**: durable event backbone for decoupled, near-real-time ingestion.
- **Spark Structured Streaming**: validation, deduplication, enrichment, and scalable stream processing.
- **Snowflake**: cloud analytical warehouse and governed data platform.
- **dbt**: version-controlled SQL transformations, tests, lineage, and marts.
- **Terraform**: Infrastructure-as-Code for reproducible cloud resources and platform configuration.

## Portfolio narrative
This project demonstrates an agentic DataOps architecture where enterprise events are streamed through Kafka, processed by Spark, landed in Snowflake, transformed and tested with dbt, and monitored by a LangGraph agent that can investigate failures and translate natural-language requests into governed data actions.

## Local quick start

1. Copy `.env.example` to `.env`.
2. Run Kafka locally:
   ```bash
   docker compose up -d
   ```
3. Install Python dependencies:
   ```bash
   python -m venv .venv
   # Windows: .venv\\Scripts\\activate
   # macOS/Linux: source .venv/bin/activate
   pip install -r requirements.txt
   ```
4. Publish sample order events:
   ```bash
   python producer/order_producer.py
   ```
5. Run the Spark job:
   ```bash
   spark-submit spark/order_stream.py
   ```
6. Configure Snowflake and run dbt:
   ```bash
   cd dbt_agentic
   dbt deps
   dbt build
   ```
7. Run the agent demo:
   ```bash
   python -m agent.app
   ```

## Safety design
The agent is read-only by default. SQL is validated before execution, sensitive columns are excluded from agent-facing semantic tools, and write/remediation actions are represented as proposals requiring explicit human approval.
