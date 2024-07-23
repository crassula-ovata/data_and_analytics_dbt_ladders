{% test child_column_not_equal_parent_column_with_null_handling(model, column_name, parent_table, parent_column_name, function_call, function_name, function_params) %}
/*
This test checks that the values in a specified column of the child table
do not match the values in a specified column of the parent table. 
Also, handles for the special case of when parent_column_name is null. 

Parameters:
  model: the child table to be tested
  column_name: the column in the child table to be checked
  parent_table: the parent table to reference
  parent_column_name: the column in the parent table to be checked
*/

{%- set params_qualified = [] %}
{%- for param in function_params %}
    {% set params_qualified = params_qualified.append('parent_case.' ~ param) %}
{%- endfor %}
{%- set params_str = params_qualified | join(', ') %}

-- Get parent table
with dm_table_data_parent as (
    select
        case_id,
        {% if not function_call %}
            {{ parent_column_name }},
        {% endif %}
        {{ function_params | join(', ') }}  -- Include params from function_params
    from
        {{ source('dm_table_data', parent_table) }}
),

-- Join model against parent table and select column_name from both
mismatched_values as (
    select
        child_case.{{ column_name }} as child_column_value,
        parent_case.{{ parent_column_name }} as parent_column_value
    from
        {{ model }} child_case
    inner join
        dm_table_data_parent parent_case
    on
        child_case.parent_case_id = parent_case.case_id
    where
        -- Handle the case where parent column is NULL
        (
            coalesce(parent_case.{{ parent_column_name }}, '') = ''
            and child_case.{{ column_name }} <> 'No information available'
        )
        or
        -- Handle the case where parent column has a value
        (
            {% if function_call %}
                (
                    coalesce(parent_case.{{ parent_column_name }}, '') <> ''
                    and child_case.{{ column_name }} <> {{ function_name }}({{ params_str }})
                )
            {% else %}
                (
                    coalesce(parent_case.{{ parent_column_name }}, '') <> ''
                    and child_case.{{ column_name }} <> parent_case.{{ parent_column_name }}
                )
            {% endif %}
        )
)

select * from mismatched_values

{% endtest %}