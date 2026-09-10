-- Run with an appropriately privileged Snowflake role.
create database if not exists AGENTIC_DATA;
create schema if not exists AGENTIC_DATA.RAW;
create schema if not exists AGENTIC_DATA.ANALYTICS;
create warehouse if not exists AGENTIC_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true;

create table if not exists AGENTIC_DATA.RAW.ORDERS (
  event_id string,
  event_type string,
  event_ts timestamp_tz,
  order_id string,
  customer_id string,
  product_id string,
  quantity integer,
  unit_price number(12,2),
  amount number(12,2),
  source_system string,
  schema_version integer,
  ingested_at timestamp_tz default current_timestamp()
);
