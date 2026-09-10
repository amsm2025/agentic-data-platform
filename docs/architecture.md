# Architecture Decisions

## Event ingestion
Kafka decouples enterprise source systems from downstream consumers and allows multiple processors to consume the same business event independently.

## Stream processing
Spark Structured Streaming performs schema validation, event-time processing, deduplication, enrichment, and anomaly routing before trusted records move downstream.

## Warehouse and modeling
Snowflake stores raw and modeled analytical data. dbt defines version-controlled transformations and tests so business logic remains reviewable and reproducible.

## Agentic operations
LangGraph coordinates diagnostic steps. The agent can inspect contracts and execute read-only SQL through explicit tools. Mutating actions are not directly exposed; remediation is proposed for human approval.

## Infrastructure
Terraform provisions repeatable AWS resources such as encrypted S3 storage and CloudWatch logging. In a production extension, the same module can provision MSK, networking, IAM, compute, and secrets.
