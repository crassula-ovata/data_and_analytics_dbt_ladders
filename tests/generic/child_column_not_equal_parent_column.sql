{% test child_column_not_equal_parent_column(model, column_name, parent_table, parent_column_name) %}
/*
This test checks that the values in a specified column of the child table
do not match the values in a specified column of the parent table.

Parameters:
  model: the child table to be tested
  column_name: the column in the child table to be checked
  parent_table: the parent table to reference
  parent_column_name: the column in the parent table to be checked
*/

-- Get parent table
with dm_table_data_parent as (
    select case_id, {{ parent_column_name }} from {{ source('dm_table_data', parent_table) }}
),

-- Join model against parent table and select column_name from both
mismatched_values as (
    select 
        child_case.{{ column_name }} as child_column_value,
        parent_case.{{ parent_column_name }} as parent_column_value

    from {{ model }} child_case 
        inner join dm_table_data_parent parent_case
        on child_case.parent_case_id = parent_case.case_id
    where 
            coalesce(child_case.{{ column_name }}, '') <> coalesce(parent_case.{{ parent_column_name }}, '')
)

select * from mismatched_values

{% endtest %}