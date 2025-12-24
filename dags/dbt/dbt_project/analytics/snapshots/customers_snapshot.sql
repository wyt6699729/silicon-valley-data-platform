{% snapshot customers_snapshot %}

{{
    config(
        target_databae='DBT_DEV',
        target_schema='snapshots',
        unique_key='c_custkey',

        strategy='check',
        check_cols=['c_address', 'c_phone', 'c_acctbal']
    )
}}

select * from {{source('tpch', 'raw_customers')}}

{% endsnapshot %}