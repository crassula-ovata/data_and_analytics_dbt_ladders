with 
util_table_data_task as (
      select * from  {{ source('util_table_data', 'TASK_LOG') }}
), 
util_table_data_execution as (
      select * from  {{ source('util_table_data', 'EXECUTION_LOG') }}
)

select * from util_table_data_task where status is null

