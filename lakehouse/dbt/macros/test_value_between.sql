{% test value_between(model, column_name, min_value, max_value) %}
-- Generic test: fails on any row whose value falls outside [min_value, max_value].
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
