{% test assert_column_less_than(model, column_name, target_column) %}
    select *
    from {{model}}
    where {{column_name}} >= {{target_column}}
{% endtest %}