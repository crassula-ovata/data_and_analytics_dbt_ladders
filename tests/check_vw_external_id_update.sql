with 
dm_table_data_clinic as (
      select * from {{ source('dm_table_data', 'CASE_CLINIC') }} where closed = 'FALSE' and external_id is not null
) 
,dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }} where closed = 'FALSE' and external_id is not null
)
,external_id_from_payload as (
    select payload:case_type::string case_type,
           payload:case_id::string case_id,
           payload:external_id::string external_id
    from {{ ref('VW_EXTERNAL_ID_UPDATE')}}
)
select * from external_id_from_payload where
    case_type='provider' and case_id not in (select case_id from dm_table_data_provider)
    and 
    case_type='clinic' and case_id not in (select case_id from dm_table_data_clinic)