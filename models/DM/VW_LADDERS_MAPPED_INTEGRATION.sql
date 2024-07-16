with 
/*
external_id_from_payload as (
    select payloadObj.value:case_type::string case_type_value, 
       payloadObj.value:case_id::string case_id,
       payloadObj.value:external_id::string external_id
    from VW_EXTERNAL_ID_API_PAYLOAD,
         lateral flatten(input => parse_json(payload)) payloadObj
) */
external_id_from_payload as (
    select payload:case_type::string case_type,
        payload:case_id::string case_id,
        payload:external_id::string external_id
    from VW_EXTERNAL_ID_UPDATE
)
,hades_table_data_mapped_active_licenses as (
      select 
        dm.external_id_format(parent_account_id) || '-' || dm.external_id_format(account_id) as new_clinic_external_id,
        dm.external_id_format(parent_account_id) as new_provider_external_id,
        * from {{ source('hades_table_data', 'VWS_LADDERS_MAPPED_ACTIVE_LICENSES') }}   
        --where 
        --legacy_parent_account_id is not null and legacy_account_id is not null
        --and parent_account_id is not null and account_id is not null
)
-- this new bhe integration view will exclude:
-- any records that has new_provider_external_id matched the external_id in VW_EXTERNAL_ID_API_PAYLOAD
-- any records that has new_clinic_external_id matched the external_id in VW_EXTERNAL_ID_API_PAYLOAD
,final as (
    select * from hades_table_data_mapped_active_licenses 
    where not exists (
        select external_id from external_id_from_payload where external_id=new_provider_external_id or external_id=new_clinic_external_id
    )
    /*
    //the following performnace isn't great
    //new_provider_external_id not in (select external_id from external_id_from_payload) 
    //and new_clinic_external_id not in (select external_id from external_id_from_payload)
    */
)
select * from final
