with secure_customers as (
    -- 1. 这里必须用 ref 指向那个 ephemeral 模型
    select * from {{ ref('stg_tpch_customers_secure') }}
)

select 
    c_custkey as customer_id,
    c_name as customer_name,
    c_address as address,
    
    -- 2. 注意：这里选的是 stg_tpch_customers_secure 里生成的那个新列名
    c_phone_masked as phone, 
    
    c_acctbal as account_balance,
    c_mktsegment as market_segment
from secure_customers