{% test split_value_not_in_fixture(model, column_name, ref_fixture) %}
/*
This test checks that the values (list of items separated by a space) in a given column, 
does not exist in the list of valid-options in the corresponding lookup-table table, or Fixture.

Parameters:
  model: the child table to be tested
  column_name: the column in the child table to be checked
  ref_fixture: the corresponding fixture, lookup-table, for the model
*/

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