{% test column_not_empty(model, column_name) %}
select count ({{ column_name }}) column_count,
count(*) total_count,
from {{ model }}
having  column_count = 0 and total_count > 0
{% endtest %}