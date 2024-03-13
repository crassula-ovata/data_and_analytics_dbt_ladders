with 
util_table_data_task as (
      select * from  {{ source('util_table_data', 'TASK_LOG') }}
), 
util_table_data_execution as (
      select * from  {{ source('util_table_data', 'EXECUTION_LOG') }}
), 
util_table_data_sql as (
      select * from  {{ source('util_table_data', 'SQL_LOGS') }}
), 
final as (
select
    case when t.status <> 'SUCCESS' or e.status <> 'SUCCESS' or s.execution_status <> 'SUCCESS' then 'FAILURE' else 'SUCCESS' end as overall_status,
    t.task_id,
    e.execution_id,
    s.sql_log_id,
    t.status as task_status,
    t.type as task_type,
    t.subtype as task_subtype,
    t.task_start,
    t.task_end,
    e.type as execution_type,
    e.subtype as exection_subtype,
    e.domain as exection_domain,
    e.status as execution_status,
    e.execution_start,
    e.execution_end,
    s.query_id,
    s.query_text as sql_query_text,
    s.database_name,
    s.schema_name,
    s.query_type,
    s.execution_status as sql_execution_status,
    s.error_code as sql_error_code,
    s.error_message as sql_error_message,
    s.start_time as sql_start_time,
    s.end_time as sql_end_time

from util_table_data_task t left join util_table_data_execution e on t.task_id = e.task_id 
     left join util_table_data_sql s on e.task_id = s.task_id and e.execution_id = s.execution_id
)

select 
	OVERALL_STATUS,
	TASK_ID,
	EXECUTION_ID,
	SQL_LOG_ID,
	TASK_STATUS,
	TASK_TYPE,
	TASK_SUBTYPE,
	TASK_START,
	TASK_END,
	EXECUTION_TYPE,
	EXECTION_SUBTYPE,
	EXECTION_DOMAIN,
	EXECUTION_STATUS,
	EXECUTION_START,
	EXECUTION_END,
	QUERY_ID,
	SQL_QUERY_TEXT,
	DATABASE_NAME,
	SCHEMA_NAME,
	QUERY_TYPE,
	SQL_EXECUTION_STATUS,
	SQL_ERROR_CODE,
	SQL_ERROR_MESSAGE,
	SQL_START_TIME,
	SQL_END_TIME
from final