{{
    config(
        materialized='ephemeral',
    )
}}

with masked as (
    select
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        '***-***-' || right(c_phone, 4) as c_phone_masked,
        c_acctbal,
        c_mktsegment
    from {{source('tpch', 'raw_customers')}}
)

select * from masked