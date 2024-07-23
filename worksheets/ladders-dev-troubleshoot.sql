use role sysadmin;

select * from DM_LADDERS_DEV.util.task_log order by task_id desc;
select * from DM_LADDERS_DEV.util.execution_log order by task_id desc;
//263 182
select * from DM_LADDERS_DEV.util.sql_logs order by sql_log_id desc;
select * from DM_LADDERS_DEV.util.message_log order by task_id desc;

select * from DM_LADDERS_DEV.util.sql_logs where execution_status <> 'SUCCESS' order by start_time desc;

-- location redesign --
select * from DM_CO_CARE_COORD_DEV.DM.location order by last_updated desc;
// 277
select * from DM_LADDERS_DEV.DM.VW_LOCATION_API_PAYLOAD;
select * from DM_LADDERS_DEV.DM.VW_LADDERS_API_PAYLOAD;
  select * from DM_CO_CARE_COORD_DEV.DM.LOCATION where location_id='d7f997cb4a414510b2ac58e988c0c6bf' order by last_updated desc;

-- hades db account name duplicate 
select * from  HADES_PROD_BHA_DIMAGI_SHARE.DM.VWS_LADDERS_ACTIVE_LICENSES where account_name='Jefferson Center for Mental Health - Independence';

select * from DM_LADDERS_DEV.DM.VW_CLINICS_CREATE_UPDATE;
  
-- tasks
use role accountadmin;
show tasks;

//drop task DM_LADDERS_DEV.UTIL.S3_UNLOAD_TASK; 

create or replace task DM_LADDERS_DEV.UTIL.S3_UNLOAD_TASK
	warehouse=COMPUTE_WH
	schedule='USING CRON 05 05 * * * America/New_York'
	as Call metadata.procedures_dev.sp_data_unload('LADDERS_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'co-carecoordination-dev', 'DM_LADDERS_DEV', 'UTIL', 'DM_LADDERS_DEV', 'UNLOAD_SF_TO_S3_GCP|', null)
;

ALTER TASK DM_LADDERS_DEV.UTIL.S3_UNLOAD_TASK RESUME;
EXECUTE TASK DM_LADDERS_DEV.UTIL.S3_UNLOAD_TASK;


// task history
select * from snowflake.account_usage.task_history where state <> 'SUCCEEDED' order by completed_time desc;
select * from snowflake.account_usage.task_history order by completed_time desc;

-- end tasks




---------------- replication issue fixes ----------------

select * from DM_LADDERS_DEV.DM.VW_LADDERS_API_PAYLOAD;
select * from DM_LADDERS_QA.DM.VW_LADDERS_API_PAYLOAD;
select * from DM_LADDERS_PROD.DM.VW_LADDERS_API_PAYLOAD;


//grant usage on schema DM_LADDERS_DEV.DM to role sysadmin;
grant ownership on schema DM_CO_CARE_COORD_DEV.DM to role sysadmin REVOKE CURRENT GRANTS;



REVOKE SELECT ON ALL views IN SCHEMA DM_LADDERS_DEV.DM from ROLE sysadmin;

grant ownership on all views in schema DM_LADDERS_DEV.DM to role sysadmin;
grant ownership on all views in schema DM_LADDERS_QA.DM to role sysadmin;
grant ownership on all views in schema DM_LADDERS_PROD.DM to role sysadmin;

grant ownership on all functions in schema DM_LADDERS_DEV.DM to role sysadmin;
grant ownership on all functions in schema DM_LADDERS_QA.DM to role sysadmin;
grant ownership on all functions in schema DM_LADDERS_PROD.DM to role sysadmin;


-- dev
REVOKE SELECT ON ALL views IN SCHEMA DM_CO_CARE_COORD_DEV.DM from ROLE DM_CO_CARE_COORD_DEV_DM_R;
grant ownership on all views in schema DM_CO_CARE_COORD_DEV.DM to role sysadmin;
grant select on all views in schema DM_CO_CARE_COORD_DEV.DM to role DM_CO_CARE_COORD_DEV_DM_R;

REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_DEV.DM from ROLE DM_CO_CARE_COORD_DEV_DM_R;
grant ownership on all tables in schema DM_CO_CARE_COORD_DEV.DM to role sysadmin;
grant select on all tables in schema DM_CO_CARE_COORD_DEV.DM to role DM_CO_CARE_COORD_DEV_DM_R;

REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_DEV.UTIL from ROLE DM_CO_CARE_COORD_DEV_UTIL_R;
grant ownership on all tables in schema DM_CO_CARE_COORD_DEV.UTIL to role sysadmin;
grant select on all tables in schema DM_CO_CARE_COORD_DEV.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;

//grant select on view DM_CO_CARE_COORD_DEV.DM.VW_CASE_COUNTS to role DM_CO_CARE_COORD_DEV_DM_R;
//grant ownership on view DM_CO_CARE_COORD_DEV.DM.VW_CASE_COUNTS to role sysadmin;

EXECUTE TASK UTIL.REPLICATION.REFRESH_DM_CARE_COORD_DEV_TASK;


-- qa
REVOKE SELECT ON ALL views IN SCHEMA DM_CO_CARE_COORD_QA.DM from ROLE DM_CO_CARE_COORD_QA_DM_R;
grant ownership on all views in schema DM_CO_CARE_COORD_QA.DM to role sysadmin REVOKE CURRENT GRANTS;
grant select on all views in schema DM_CO_CARE_COORD_QA.DM to role DM_CO_CARE_COORD_QA_DM_R;
show grants on view DM_CO_CARE_COORD_QA.DM.VW_CLINIC_PROVIDER_CAPACITY;

REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_QA.DM from ROLE DM_CO_CARE_COORD_QA_DM_R;
//REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_QA.DM from ROLE EA37219.BHA_UAT_S;
grant ownership on all tables in schema DM_CO_CARE_COORD_QA.DM to role sysadmin REVOKE CURRENT GRANTS;;
grant select on all tables in schema DM_CO_CARE_COORD_QA.DM to role DM_CO_CARE_COORD_QA_DM_R;
show grants on view DM_CO_CARE_COORD_QA.DM.CASE_ALIAS;

REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_QA.UTIL from ROLE DM_CO_CARE_COORD_QA_UTIL_R;
grant ownership on all tables in schema DM_CO_CARE_COORD_QA.UTIL to role sysadmin;
grant select on all tables in schema DM_CO_CARE_COORD_QA.UTIL to role DM_CO_CARE_COORD_QA_UTIL_R;

EXECUTE TASK UTIL.REPLICATION.REFRESH_DM_CARE_COORD_QA_TASK;

-- PROD
REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_PROD.UTIL from ROLE DM_CO_CARE_COORD_PROD_LOG_R;
REVOKE SELECT ON ALL tables IN SCHEMA DM_CO_CARE_COORD_PROD.UTIL from ROLE DM_CO_CARE_COORD_PROD_UTIL_R;
grant ownership on all tables in schema DM_CO_CARE_COORD_PROD.UTIL to role sysadmin;
grant select on all tables in schema DM_CO_CARE_COORD_PROD.UTIL to role DM_CO_CARE_COORD_PROD_LOG_R;
grant select on all tables in schema DM_CO_CARE_COORD_PROD.UTIL to role DM_CO_CARE_COORD_PROD_UTIL_R;



//grant select on all tables in schema dm_co_care_coord_prod.dl to share bha_prod_s;
//grant select on all tables in schema dm_co_care_coord_prod.dm to share bha_prod_s;






select * from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC where
case_name='Set Apart Treatment Inc'
and closed=false
;





select c.case_id, c.external_id, l.site_code, l.location_id, l.location_type_code from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC as c
left outer join DM_CO_CARE_COORD_DEV.DM.LOCATION as l
on c.external_id = l.site_code
where l.site_code is NULL
and c.external_id is not null
and c.closed = 'false';-- and l.location_type_code = 'facility';


with ld as (
    select * 
    ,dm.external_id_format (parent_account_id) p_id
    ,dm.external_id_format (account_id) a_id
    from HADES_PROD_BHA_DIMAGI_SHARE.DM.VWS_LADDERS_ACTIVE_LICENSES
),
l as (
    select * from DM_CO_CARE_COORD_DEV.DM.LOCATION
)
select c.case_name, c.case_id, c.external_id, l.site_code, l.location_id 
from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC c 
        left join ld on ld.p_id || '-' || ld.a_id = c.external_id
        left join l on l.site_code = split_part(c.external_id, '-', 1) and l.location_type_code = 'organization'
        left join l fac on fac.site_code = c.external_id and fac.location_type_code = 'facility'
    where --ld.p_id is null and 
    fac.location_id is null 
        and c.external_id is not null and l.location_id is not null
        and closed=false;

select ld.parent_account_id,ld.p_id, ld.account_name,c.case_id, c.external_id, l.location_id from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC as c
  left join ld on ld.p_id || '-' || ld.a_id = c.external_id
  left join l on c.external_id = l.site_code and l.location_type_code = 'facility'
where l.site_code is NULL
and c.external_id is not null
--and ld.p_id is null 
and l.location_id is null 
and c.closed = false;


select case_name, c.case_id, c.external_id, l.site_code, l.location_id, l.location_type_code from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC as c
left outer join DM_CO_CARE_COORD_DEV.DM.LOCATION as l
on c.external_id = l.site_code
where l.site_code is NULL
and c.external_id is not null
and c.closed = 'false';

---

select case_id, display_name, external_id from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC
where owner_id = '6fc5b9a0b5eb477f81fcd9cb0942cd12' and closed = 'false';

select case_id, case_name, external_id from DM_CO_CARE_COORD_DEV.DM.CASE_PROVIDER
where owner_id = 'e92d7e31a2a844bca8529c28a05b938e' and closed = 'false' and case_name != 'Boulder Alcohol Education Center - General Account';


select case_id, server_date_modified, service_types, clinic_type from DM_CO_CARE_COORD_dev.DM.CASE_CLINIC where closed=false and (clinic_type is null or service_types is null);