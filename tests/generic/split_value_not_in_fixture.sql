{% test split_value_not_in_fixture(model, column_name, ref_fixture) %}

with 
dm_table_data_fixture as (
    select * from  {{ source('dm_table_data', ref_fixture) }}
),

cte_flattened_values as 
(
    select
        flat_values.value::string as flat_value
    from  {{ model }} ,
        lateral flatten(input => split({{ column_name }}, ' ')) as flat_values
    where
        {{ column_name }} || '_action' = {{ column_name }}
) 
select * 
from cte_flattened_values
where 
 NOT EXISTS (SELECT 1 FROM dm_table_data_fixture fx where fx.value = cte_flattened_values.flat_value )

{% endtest %}