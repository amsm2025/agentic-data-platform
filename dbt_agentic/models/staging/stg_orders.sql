with source as (
    select * from {{ source('raw', 'orders') }}
)
select
    event_id,
    event_ts,
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    amount,
    source_system,
    schema_version
from source
where event_id is not null
