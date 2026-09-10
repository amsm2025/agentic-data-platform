# Agentic DataOps Platform

A portfolio-grade Senior Agentic Data Engineer project that combines **LangGraph, OpenAI, Apache Kafka, Apache Spark Structured Streaming, Snowflake, dbt, Terraform, Docker, Python, and automated testing** into one governed end-to-end data platform.

The project demonstrates how an AI-powered DataOps agent can operate safely above a modern streaming and analytics stack to inspect pipeline health, query governed datasets, investigate quarantined records, reconcile analytics outputs, and propose remediation while destructive database actions remain blocked.

---

## Business Scenario

A multi-system enterprise receives customer, order, inventory, HR, CRM, and product events from upstream operational applications.

The platform must:

- ingest events in near real time,
- validate incoming records,
- detect invalid or anomalous data,
- deduplicate streaming events,
- quarantine failed records,
- persist trusted records into Snowflake,
- transform warehouse data with dbt,
- generate analytics-ready marts,
- validate data quality automatically,
- expose governed analytics to an AI agent,
- diagnose pipeline issues using natural language,
- inspect dead-letter queue records,
- reconcile raw and transformed data,
- and protect the platform from destructive AI-generated database operations.

The current implementation focuses on an end-to-end **order processing pipeline**.

---

## Implementation Status

The current implementation has been validated end-to-end.

```text
Python Order Producer
        |
        v
Kafka: orders.raw
        |
        v
Spark Structured Streaming
        |
        +-------------------------------+
        |                               |
        | VALID                         | INVALID
        v                               v
Snowflake                           Kafka
RAW.ORDERS                          orders.dlq
        |
        v
dbt
        |
        +--> ANALYTICS.STG_ORDERS
        |
        +--> ANALYTICS.FCT_SALES
                    |
                    v
          LangGraph DataOps Agent
                    |
                    +--> OpenAI tool calling
                    +--> read-only Snowflake SQL
                    +--> pipeline contract inspection
                    +--> Kafka DLQ inspection
                    +--> raw-to-mart reconciliation
                    +--> governed diagnostics
```

### Verified Capabilities

The following capabilities have been implemented and tested:

- Python event generation
- Kafka event publishing
- Kafka `orders.raw` ingestion
- Spark Structured Streaming consumption
- JSON schema parsing
- required-field validation
- quantity validation
- amount validation
- calculated amount reconciliation
- streaming event deduplication
- watermark-based stream handling
- valid/invalid data classification
- valid record routing to Snowflake
- invalid record routing to Kafka DLQ
- Snowflake RAW data persistence
- dbt staging model creation
- dbt analytics mart creation
- dbt automated data-quality testing
- LangGraph agent orchestration
- OpenAI tool calling
- read-only Snowflake SQL execution
- pipeline contract inspection
- Kafka DLQ inspection
- pipeline health diagnostics
- raw-to-mart reconciliation
- destructive SQL blocking
- human approval boundary for write/remediation actions
- automated Python testing
- live Snowflake integration testing

Current automated Python test status:

```text
11 passed
```

Current dbt data test status:

```text
5 passed
```

---

## High-Level Architecture

```text
ERP / CRM / HRIS / Product APIs
              |
              v
 Python Producers / API Connectors
              |
              v
         Apache Kafka
              |
              v
   Spark Structured Streaming
     - schema validation
     - data-quality checks
     - deduplication
     - enrichment
     - anomaly routing
              |
              +---------------------> Kafka DLQ
              |                       orders.dlq
              |
              v
       Snowflake RAW Layer
              |
              v
             dbt
       STAGING -> MARTS
              |
              v
       BI / Analytics Layer
              ^
              |
      LangGraph DataOps Agent
       - inspect health
       - query Snowflake
       - inspect contracts
       - inspect DLQ
       - reconcile data
       - explain anomalies
       - propose remediation
       - require approval for writes
```

---

## Detailed Data Flow

### 1. Python Event Producer

The platform begins with a Python order producer.

The producer generates structured order events containing fields such as:

```text
event_id
event_type
event_ts
order_id
customer_id
product_id
quantity
unit_price
amount
source_system
schema_version
```

Example logical event:

```json
{
  "event_id": "2ea2f3e5-7d44-4a97-89d2-123456789abc",
  "event_type": "order_created",
  "event_ts": "2026-09-10T14:30:00+00:00",
  "order_id": "ORD-10001",
  "customer_id": "CUST-102",
  "product_id": "PROD-42",
  "quantity": 2,
  "unit_price": 249.50,
  "amount": 499.00,
  "source_system": "ECOM",
  "schema_version": 1
}
```

Events are published to:

```text
orders.raw
```

---

## Apache Kafka Layer

Kafka acts as the durable streaming backbone.

Current topics:

```text
orders.raw
orders.dlq
```

### `orders.raw`

Contains newly generated order events awaiting streaming processing.

### `orders.dlq`

Contains invalid or quarantined events rejected by the Spark data-quality pipeline.

This allows the platform to keep failed records available for investigation without allowing them to contaminate trusted warehouse data.

---

## Spark Structured Streaming

Apache Spark Structured Streaming consumes events from Kafka.

The Spark job performs:

- Kafka event ingestion
- JSON parsing
- schema enforcement
- required-field validation
- timestamp conversion
- quantity validation
- amount validation
- expected amount calculation
- amount variance calculation
- data-quality classification
- event watermarking
- deduplication
- valid event routing
- invalid event routing

The stream separates records into two logical paths:

```text
                 Spark
                   |
          +--------+--------+
          |                 |
        VALID             INVALID
          |                 |
          v                 v
Snowflake RAW.ORDERS   Kafka orders.dlq
```

---

## Data Quality Rules

Order events are checked before entering the trusted warehouse path.

Current logical validation rules include:

```text
event_id must be present
event_ts must be present
order_id must be present
customer_id must be present
product_id must be present
quantity must be greater than zero
amount must be greater than or equal to zero
source_system must be present
schema_version must be valid
```

The stream also calculates:

```text
expected_amount = quantity * unit_price
```

and compares the expected amount with the received event amount.

Records receive:

```text
dq_status
dq_reason
```

Example valid classification:

```text
dq_status = VALID
dq_reason = NULL
```

Example invalid classification:

```text
dq_status = INVALID
dq_reason = invalid_quantity
```

---

## Valid Record Path

Valid records are written by Spark into Snowflake:

```text
AGENTIC_DATA.RAW.ORDERS
```

The RAW table contains order-level fields such as:

```text
EVENT_ID
EVENT_TYPE
EVENT_TS
ORDER_ID
CUSTOMER_ID
PRODUCT_ID
QUANTITY
UNIT_PRICE
AMOUNT
SOURCE_SYSTEM
SCHEMA_VERSION
EXPECTED_AMOUNT
AMOUNT_VARIANCE
DQ_STATUS
DQ_REASON
PROCESSED_AT
```

This table represents trusted order records accepted by the streaming pipeline.

---

## Dead-Letter Queue

Invalid records are written to:

```text
orders.dlq
```

The DLQ keeps:

- the original raw event,
- Kafka metadata,
- the data-quality result,
- and the validation failure reason.

A tested invalid event included:

```text
quantity = -2
amount = -200
dq_status = INVALID
dq_reason = invalid_quantity
```

The record was successfully quarantined and excluded from Snowflake `RAW.ORDERS`.

This proves that the pipeline separates trusted and invalid records instead of simply loading everything into the warehouse.

---

## Snowflake Architecture

Snowflake is used as the analytical warehouse.

Current database:

```text
AGENTIC_DATA
```

Current schemas:

```text
AGENTIC_DATA.RAW
AGENTIC_DATA.ANALYTICS
```

### RAW Layer

Streaming records land in:

```text
AGENTIC_DATA.RAW.ORDERS
```

### Analytics Layer

dbt creates analytics-ready objects inside:

```text
AGENTIC_DATA.ANALYTICS
```

---

## dbt Transformation Layer

dbt transforms trusted Snowflake data into analytics-ready models.

Current implemented models:

```text
AGENTIC_DATA.ANALYTICS.STG_ORDERS
AGENTIC_DATA.ANALYTICS.FCT_SALES
```

### `STG_ORDERS`

The staging layer prepares trusted RAW order data for downstream analytics.

### `FCT_SALES`

The sales mart aggregates order information by date and source system.

Current analytical columns include:

```text
ORDER_DATE
SOURCE_SYSTEM
ORDER_COUNT
UNITS_SOLD
GROSS_SALES
```

---

## dbt Data Testing

The dbt project includes automated data-quality checks.

Current tests cover areas such as:

- `ORDER_ID` not null
- `EVENT_ID` not null
- `EVENT_ID` uniqueness
- sales date not null
- gross sales not null

Current result:

```text
PASS=5
WARN=0
ERROR=0
```

---

## Proven Raw-to-Mart Reconciliation

The platform has successfully reconciled the Snowflake RAW layer with the dbt sales mart.

A validated run produced:

```text
RAW.ORDERS
------------------------
Valid orders: 25
Units:        71
Gross sales:  10,130.39
```

The corresponding analytics mart produced:

```text
FCT_SALES
------------------------
Orders:       25
Units:        71
Gross sales:  10,130.39
```

Variance:

```text
Orders variance:      0
Units variance:       0
Gross sales variance: 0
```

This demonstrates that the transformed analytics layer reconciles with the trusted streaming source data.

---

## LangGraph DataOps Agent

The project includes a governed AI DataOps agent built with LangGraph.

The agent can reason about pipeline health while interacting with external systems only through explicitly defined tools.

The current agent architecture is:

```text
User Question
      |
      v
LangGraph Agent
      |
      v
OpenAI Model
      |
      +------------------------------+
      |              |               |
      v              v               v
Snowflake Tool   Contract Tool    DLQ Tool
      |              |               |
      v              v               v
Read-only SQL    Pipeline rules    Kafka records
      |
      v
Governed Response
```

---

## Agent Tools

The agent currently exposes three governed tools.

### `run_readonly_sql`

Runs approved read-only Snowflake SQL.

Supported SQL command families include:

```sql
SELECT
WITH
SHOW
DESCRIBE
DESC
```

The tool limits result retrieval and obtains Snowflake credentials from environment variables.

The language model does not receive direct database credentials.

---

### `get_pipeline_contract`

Returns the logical contract for the order processing pipeline.

The agent can use this tool to understand:

- required fields,
- accepted values,
- validation rules,
- deduplication requirements,
- valid destination,
- and invalid destination.

This prevents the model from having to invent pipeline behavior.

---

### `inspect_dlq`

Reads records from:

```text
orders.dlq
```

without modifying them and without committing consumer offsets.

The agent can therefore inspect quarantined records and explain why a record failed validation.

---

## Example Agent Questions

The agent can answer questions such as:

```text
How many source systems are represented in the sales mart?
```

```text
Which source system has the highest gross sales, and what is the amount?
```

```text
What data quality rules should an order event satisfy before it is considered valid?
```

```text
Are there any invalid or quarantined order records in the current pipeline?
```

```text
Compare the order pipeline contract with the current sales mart and tell me whether the mart appears healthy.
```

```text
Give me a concise health summary of the order pipeline, including valid records, sales mart reconciliation, and DLQ status.
```

---

## Example Agent Diagnostics

The agent has successfully identified:

```text
3 source systems
```

and determined that:

```text
ECOM
```

was the highest-performing source system in a tested sales dataset.

It has also successfully:

- reconciled RAW and mart order counts,
- reconciled RAW and mart unit totals,
- reconciled RAW and mart gross sales,
- identified quarantined events,
- explained their validation failures,
- and distinguished between invalid warehouse records and DLQ records.

---

## Governed AI Safety Model

The DataOps agent follows a defense-in-depth safety design.

### Read-Only by Default

The model does not receive unrestricted Snowflake access.

Database operations must go through the governed SQL tool.

---

## Deterministic SQL Guard

SQL is checked before database execution.

Allowed examples:

```sql
SELECT *
FROM AGENTIC_DATA.ANALYTICS.FCT_SALES;
```

```sql
SHOW TABLES;
```

Blocked command families include:

```sql
INSERT
UPDATE
DELETE
MERGE
DROP
ALTER
TRUNCATE
GRANT
REVOKE
CREATE
REPLACE
```

---

## Destructive SQL Protection

The following type of request is intentionally rejected:

```sql
DELETE FROM AGENTIC_DATA.ANALYTICS.FCT_SALES;
```

The SQL execution tool raises:

```text
ValueError: Only read-only SQL is allowed
```

This provides a deterministic enforcement layer independent of the LLM's own reasoning.

---

## Human Approval Boundary

The agent may:

- investigate,
- diagnose,
- explain,
- recommend,
- and propose remediation.

It must not directly perform destructive write actions.

The architecture therefore separates:

```text
AI reasoning authority
```

from:

```text
production modification authority
```

Write or remediation actions require explicit approval outside the current agent execution path.

---

## Testing Strategy

The project includes automated Python testing.

Current suite:

```text
11 passed
```

Tests cover:

- valid SELECT SQL
- valid WITH SQL
- valid SHOW SQL
- valid DESCRIBE SQL
- blocked DELETE
- blocked DROP
- blocked UPDATE
- blocked CREATE
- DLQ tool existence
- DLQ tool metadata
- live Snowflake read-only integration

The project also uses:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
```

to keep Spark runtime utilities from being incorrectly collected as unit tests.

---

## Snowflake Integration Testing

A live integration test verifies that the DataOps SQL tool can connect to Snowflake and query:

```text
AGENTIC_DATA.ANALYTICS.FCT_SALES
```

The test confirms that:

- environment variables are available,
- Snowflake authentication works,
- the governed SQL tool works,
- the mart exists,
- and analytics data can be read successfully.

---

## Why Each Technology Is Used

### Python

Used for:

- event generation,
- Kafka integration,
- agent tools,
- Snowflake access,
- test automation,
- and orchestration logic.

### LangGraph

Used for stateful agent orchestration.

It manages:

- model execution,
- tool calls,
- tool responses,
- agent state,
- and controlled diagnostic workflows.

### OpenAI

Provides the LLM reasoning and tool-calling layer.

The model is bound to governed tools instead of having unrestricted access to infrastructure.

### Apache Kafka

Provides the streaming event backbone.

Kafka decouples event producers from stream-processing consumers.

### Apache Spark Structured Streaming

Provides scalable continuous stream processing.

It performs:

- validation,
- transformation,
- enrichment,
- deduplication,
- data-quality classification,
- and multi-destination routing.

### Snowflake

Acts as the analytical warehouse and trusted persistence layer.

It separates RAW and ANALYTICS schemas for clearer data-layer boundaries.

### dbt

Provides version-controlled SQL transformations and automated warehouse testing.

It turns trusted raw data into analytics-ready models.

### Terraform

Provides Infrastructure-as-Code foundations for future AWS deployment and repeatable cloud provisioning.

### Docker

Provides reproducible Kafka and Spark runtime environments.

Docker also avoids Windows-specific Spark/Hadoop filesystem compatibility issues.

### pytest

Provides repeatable automated Python testing for safety controls, tools, and live integration behavior.

---

## Local Development Architecture

Development currently uses:

```text
Windows Host
     |
     +--> Python virtual environment
     |
     +--> Python producer
     |
     +--> LangGraph DataOps agent
     |
     +--> dbt
     |
     +--> Snowflake
     |
     +--> Docker
            |
            +--> Kafka
            |
            +--> Spark
```

Kafka exposes separate listener paths so that both the Windows host and Docker-based Spark service can communicate correctly.

Host producer:

```text
localhost:9092
```

Docker Spark:

```text
kafka:29092
```

---

## Why Spark Runs in Docker

Initial Spark execution on Windows successfully connected to Kafka but encountered a Hadoop native filesystem checkpoint issue.

The failure involved Windows Hadoop native I/O.

Rather than depending on unofficial Windows Hadoop binaries, the project runs Spark inside a Linux-based Docker container.

This provides:

- reliable checkpoint handling,
- Linux filesystem semantics,
- reproducible execution,
- easier team onboarding,
- and better deployment portability.

---

## Local Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/amsm2025/agentic-data-platform.git
cd agentic-data-platform
```

---

### 2. Create the Python Virtual Environment

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
source .venv/bin/activate
```

---

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure Environment Variables

Copy:

```text
.env.example
```

to:

```text
.env
```

Configure the required local credentials.

Typical variables include:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE
OPENAI_API_KEY
KAFKA_BOOTSTRAP_SERVERS
KAFKA_ORDER_TOPIC
KAFKA_DLQ_TOPIC
```

Never commit `.env`.

The repository intentionally excludes `.env` through `.gitignore`.

---

## Start Kafka and Spark

Run:

```bash
docker compose up --build
```

The compose environment starts the local streaming infrastructure.

---

## Kafka Topics

Create or verify:

```text
orders.raw
orders.dlq
```

Kafka currently uses three partitions per topic in the development setup.

---

## Publish Sample Events

Run:

```bash
python producer/order_producer.py
```

The producer generates sample order events and publishes them to:

```text
orders.raw
```

---

## Configure Snowflake

Snowflake bootstrap SQL is located in:

```text
snowflake/bootstrap.sql
```

The project uses an X-Small warehouse for development:

```text
AGENTIC_WH
```

Database:

```text
AGENTIC_DATA
```

Schemas:

```text
RAW
ANALYTICS
```

---

## Run dbt

Navigate to:

```bash
cd dbt_agentic
```

Validate connectivity:

```bash
dbt debug
```

Build models:

```bash
dbt run
```

Run tests:

```bash
dbt test
```

Expected validated result:

```text
PASS=5
WARN=0
ERROR=0
```

Return to the project root:

```bash
cd ..
```

---

## Run the DataOps Agent

Run:

```bash
python -m agent.app
```

The CLI prompts:

```text
Ask the DataOps agent:
```

Example:

```text
Ask the DataOps agent: Which source system has the highest gross sales?
```

---

## Run Automated Python Tests

Run:

```bash
python -m pytest -v
```

Current validated result:

```text
11 passed
```

---

## Project Structure

```text
agentic-data-platform/
|
+-- agent/
|   +-- __init__.py
|   +-- app.py
|   +-- tools.py
|
+-- producer/
|   +-- order_producer.py
|
+-- spark/
|   +-- order_stream.py
|   +-- snowflake_smoke_test.py
|
+-- snowflake/
|   +-- bootstrap.sql
|
+-- dbt_agentic/
|   +-- models/
|   |   |
|   |   +-- staging/
|   |   |   +-- sources.yml
|   |   |   +-- stg_orders.sql
|   |   |
|   |   +-- marts/
|   |       +-- fct_sales.sql
|   |       +-- schema.yml
|   |
|   +-- dbt_project.yml
|   +-- profiles.yml.example
|
+-- terraform/
|   +-- versions.tf
|   +-- variables.tf
|   +-- main.tf
|
+-- tests/
|   +-- test_sql_guard.py
|   +-- test_dlq_tool.py
|   +-- test_snowflake_readonly_integration.py
|
+-- docs/
|   +-- architecture.md
|
+-- docker-compose.yml
+-- requirements.txt
+-- pytest.ini
+-- test_snowflake.py
+-- .env.example
+-- .gitignore
+-- README.md
```

---

## Security and Secret Management

The project follows several repository security practices.

### Secrets Are Not Committed

Local secrets are stored in:

```text
.env
```

The file is excluded from Git.

### Example Configuration Only

The repository includes:

```text
.env.example
```

with placeholders rather than production credentials.

### Runtime Credentials

Snowflake and OpenAI credentials are loaded from environment variables.

### Credential Rotation

Development credentials can be rotated without changing source code because credentials are not hard-coded into application logic.

---

## Current `.gitignore` Strategy

Generated or private files are excluded, including:

```text
.env
.venv/
__pycache__/
.pytest_cache/
target/
dbt_packages/
logs/
*.tfstate
*.tfstate.*
.terraform/
data/
dbt_agentic/.user.yml
```

The `data/` directory contains Spark runtime data such as:

- checkpoints,
- offsets,
- commit logs,
- state files,
- and Ivy dependency caches.

These files are runtime artifacts and should not be committed.

---

## Infrastructure as Code

Terraform is included as the foundation for cloud infrastructure.

Current Terraform scope includes:

- AWS provider configuration
- S3 foundation
- S3 versioning
- S3 encryption
- CloudWatch logging
- regional configuration

Target AWS region:

```text
ap-southeast-1
```

Future infrastructure expansion may include:

- Amazon MSK
- networking
- IAM
- secrets management
- compute
- monitoring
- alerting
- and managed deployment infrastructure.

---

## Design Principles

The project follows several core architectural principles.

### 1. Separate Streaming from Analytics

Kafka and Spark handle event movement and stream validation.

Snowflake and dbt handle governed analytics transformation.

### 2. Quarantine Invalid Data

Bad records are retained in a DLQ rather than silently discarded or loaded into trusted tables.

### 3. AI Does Not Bypass Governance

The LLM must use explicitly defined tools.

### 4. Read-Only AI by Default

The agent cannot directly modify warehouse objects or records.

### 5. Deterministic Safety Before Execution

SQL safety rules are enforced in application code before Snowflake execution.

### 6. Human-in-the-Loop Remediation

The agent may recommend corrective actions but write operations require external authorization.

### 7. Test the Platform, Not Only the Model

The project validates:

- SQL safety controls,
- Snowflake connectivity,
- Kafka DLQ tooling,
- dbt data models,
- and end-to-end reconciliation.

---

## Agentic Data Engineering Pattern

The key architectural pattern demonstrated by the project is:

```text
Data Infrastructure
        +
Governed AI Tools
        +
Agent Reasoning
        +
Deterministic Safety Controls
        +
Human Approval
        =
Agentic DataOps
```

The AI agent does not replace the data platform.

Instead, it operates above the data platform as a controlled intelligence layer.

---

## Portfolio Narrative

This project demonstrates an end-to-end agentic data engineering architecture where enterprise events are generated with Python, streamed through Kafka, validated and routed using Spark Structured Streaming, stored in Snowflake, transformed and tested with dbt, and investigated through a LangGraph-powered DataOps agent.

The agent combines natural-language reasoning with governed tools for Snowflake queries, pipeline contract inspection, and Kafka DLQ diagnostics.

The implementation emphasizes that AI should not be granted unrestricted infrastructure authority.

Instead, the system applies explicit tooling, read-only access, deterministic SQL protection, automated testing, and human approval boundaries.

---

## Skills Demonstrated

This project demonstrates hands-on experience with:

- Agentic AI
- AI tool calling
- LangGraph
- OpenAI APIs
- Python
- SQL
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- Snowflake
- dbt
- data pipelines
- streaming data engineering
- event-driven architecture
- schema validation
- data-quality engineering
- data reconciliation
- dead-letter queues
- data warehousing
- analytics marts
- Infrastructure-as-Code
- Terraform
- AWS foundations
- Docker
- distributed systems
- pytest
- automated testing
- integration testing
- safety guardrails
- read-only AI tooling
- human-in-the-loop workflows
- DataOps
- AI-assisted operations
- cloud data engineering

---

## Current Technology Stack

```text
Language
-------
Python
SQL

Agentic AI
----------
LangGraph
OpenAI
LangChain tools

Streaming
---------
Apache Kafka
Apache Spark Structured Streaming

Warehouse
---------
Snowflake

Transformation
--------------
dbt

Infrastructure
--------------
Terraform
AWS foundations

Runtime
-------
Docker
Docker Compose

Testing
-------
pytest
dbt tests

Version Control
---------------
Git
GitHub
```

---

## Current Verified Platform State

The project has demonstrated the following full data path:

```text
Python Producer
      |
      v
Kafka orders.raw
      |
      v
Spark Structured Streaming
      |
      +----------------------+
      |                      |
      v                      v
VALID                     INVALID
      |                      |
      v                      v
Snowflake RAW.ORDERS     Kafka orders.dlq
      |
      v
dbt STG_ORDERS
      |
      v
dbt FCT_SALES
      |
      v
LangGraph DataOps Agent
      |
      +--> Snowflake diagnostics
      +--> pipeline contract inspection
      +--> DLQ inspection
      +--> analytics reconciliation
      +--> governed response
```

---

## Roadmap

Planned enhancements include:

- Terraform-managed AWS networking
- Amazon MSK deployment
- managed cloud Spark processing
- secrets management
- richer observability
- structured pipeline telemetry
- CloudWatch monitoring
- automated alerting
- data lineage metadata
- PII masking
- role-based agent permissions
- more advanced SQL parsing
- idempotent Snowflake ingestion
- Snowflake `MERGE`-based streaming writes
- multi-reason DQ classification
- malformed JSON classification
- event type validation
- schema evolution handling
- richer Kafka DLQ diagnostics
- latest-record DLQ inspection
- automated remediation proposals
- approval workflow integration
- production deployment pipelines
- CI/CD
- agent evaluation
- prompt and tool-call observability
- additional enterprise source connectors
- CRM integration
- ERP integration
- HRIS integration
- product event integration

---

## Future Target Architecture

```text
Enterprise Systems
 ERP | CRM | HRIS | Product APIs
             |
             v
       API / Event Layer
             |
             v
        Amazon MSK
             |
             v
   Managed Spark Processing
             |
      +------+------+
      |             |
      v             v
 Trusted        Quarantine
   Data             DLQ
      |
      v
   Snowflake
      |
      v
      dbt
      |
      v
Analytics / BI / Semantic Layer
      ^
      |
      |
LangGraph DataOps Agent
      |
      +--> Metadata tools
      +--> Snowflake tools
      +--> DLQ tools
      +--> lineage tools
      +--> monitoring tools
      +--> remediation proposals
      |
      v
Human Approval / Operations
```

---

## Repository

GitHub repository:

```text
https://github.com/amsm2025/agentic-data-platform
```

---

## Project Purpose

This repository is designed as both:

1. a working engineering demonstration, and
2. a portfolio project for senior data engineering and agentic AI roles.

It demonstrates how traditional data engineering technologies can be combined with governed AI agents to create intelligent operational data platforms without sacrificing safety, auditability, data quality, or human oversight.

---

## License

This project is currently maintained as a portfolio and demonstration project.