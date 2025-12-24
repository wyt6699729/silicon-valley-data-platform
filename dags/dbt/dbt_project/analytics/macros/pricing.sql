{% macro discounted_amount(column_name, discount_rate=0.10) %}
    ({{column_name}} * (1 - {{discount_rate}})) :: numeric(16, 2)
{% endmacro %}