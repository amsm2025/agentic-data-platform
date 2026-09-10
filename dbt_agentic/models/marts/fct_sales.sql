select
    date_trunc('day', event_ts) as order_date,
    source_system,
    count(distinct order_id) as order_count,
    sum(quantity) as units_sold,
    sum(amount) as gross_sales
from {{ ref('stg_orders') }}
group by 1, 2
