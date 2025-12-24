select
    order_key,
    total_price
from {{ref('stg_tpch_orders')}}
where total_price < 0