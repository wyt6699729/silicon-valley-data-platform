{{
    config(
        materialized='incremental',
        unique_key='order_key'
    )
}}

-- 注释写在这里是安全的 (SQL区域)
with orders as (
    select * from {{ ref('stg_tpch_orders') }}
)

select 
    order_key,
    customer_key,
    status,
    total_price,
    order_date,
    current_timestamp() as dbt_updated_at
from orders

{% if is_incremental() %}

  -- 这里也是安全的 (SQL区域)
  -- 只有当订单日期大于目标表里现有的最大日期时，才选取
  where order_date > (select max(order_date) from {{ this }})

{% endif %}