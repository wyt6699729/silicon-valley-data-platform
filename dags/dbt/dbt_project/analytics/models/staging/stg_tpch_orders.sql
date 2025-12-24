select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status,
    o_totalprice as total_price,
    o_orderdate as order_date,
    o_totalprice as original_price,

    {{discounted_amount('o_totalprice')}} as discounted_price
from {{ source('tpch', 'raw_orders') }}

{% if target.name == 'dev' %}

    limit 100
{% endif %}