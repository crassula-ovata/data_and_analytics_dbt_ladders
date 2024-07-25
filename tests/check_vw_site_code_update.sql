with 
dm_table_data_location as (
    select * from {{ source('dm_table_data', 'LOCATION') }} where site_code is not null
)
,site_code_update as (
    select 
      loc_id,
      payload:location_id::string locaton_id_payload
    from {{ ref('VW_SITE_CODE_UPDATE')}}
)
select * from site_code_update where
    locaton_id_payload not in (select location_id from dm_table_data_location)