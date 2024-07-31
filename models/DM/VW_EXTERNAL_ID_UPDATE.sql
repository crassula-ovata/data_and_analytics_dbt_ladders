with 
dm_table_data_clinic as (
      select 
        replace(external_id, '_', '') as external_id_compare,
        * 
      from {{ source('dm_table_data', 'CASE_CLINIC') }} where closed = 'FALSE' and external_id is not null
) 
,dm_table_data_provider as (
      select 
        replace(external_id, '_', '') as external_id_compare,
        * 
      from  {{ source('dm_table_data', 'CASE_PROVIDER') }} where closed = 'FALSE' and external_id is not null
)
,hades_table_data_mapped_active_licenses as (
      select 
        replace(dm.external_id_format(legacy_parent_account_id), '_', '') || '-' || replace(dm.external_id_format(legacy_account_id), '_', '') as legacy_clinic_external_id,
        dm.external_id_format(parent_account_id) || '-' || dm.external_id_format(account_id) as new_clinic_external_id,
        replace(dm.external_id_format(legacy_parent_account_id), '_', '') as legacy_provider_external_id,
        dm.external_id_format(parent_account_id) as new_provider_external_id,
        * from {{ source('hades_table_data', 'VWS_LADDERS_MAPPED_ACTIVE_LICENSES') }}
      where legacy_parent_account_id is not null and legacy_account_id is not null
      and parent_account_id is not null and account_id is not null
)
,legacy_records_in_bhe as (
    select * from hades_table_data_mapped_active_licenses where parent_account_id=legacy_parent_account_id and account_id=legacy_account_id
)
-- not in legacy_records_in_bhe
-- legacy_provider_external_id = provider case's external_id
,provider_case_update as (
select 
    legacy_provider_external_id,
    new_provider_external_id,
    dm_table_data_provider.external_id_compare,
    dm_table_data_provider.external_id,
    dm_table_data_provider.case_id,
    dm_table_data_provider.case_type,
    dm_table_data_provider.case_name,
    current_timestamp() as import_date
 from hades_table_data_mapped_active_licenses bhe    
    inner join dm_table_data_provider on legacy_provider_external_id=dm_table_data_provider.external_id_compare
    where bhe.account_id not in (select account_id from legacy_records_in_bhe)
) 
,provider_case_update_payload as (
select 
    CASE_ID,
    CASE_TYPE, 
    parse_json(
            '{ "create": false' || ', ' ||  
            '"case_type": "provider",' || 
            '"case_id": ' || '"' || case_id || '", ' ||
            '"external_id": ' || '"' || new_provider_external_id || '", ' ||
            '"case_name": ' || '"' || CASE_NAME  || '",' || 
            '"properties": {' ||
                '"import_date": ' || '"' || replace(replace(replace(IMPORT_DATE::string, '"', '\\"'), '\n', '\\n'), '\r', '\\r') || '"' || 
            ' } }') payload 
    from provider_case_update
)
-- not in legacy_records_in_bhe
-- legacy_clinic_external_id = clinic case's external_id
,clinic_case_update as (
select 
    legacy_clinic_external_id,
    new_clinic_external_id,
    dm_table_data_clinic.external_id_compare,
    dm_table_data_clinic.external_id,
    dm_table_data_clinic.case_id,
    dm_table_data_clinic.case_type,
    dm_table_data_clinic.case_name,
    current_timestamp() as import_date
 from hades_table_data_mapped_active_licenses bhe    
    inner join dm_table_data_clinic on legacy_clinic_external_id=dm_table_data_clinic.external_id_compare
    where bhe.account_id not in (select account_id from legacy_records_in_bhe)
) 
,clinic_case_update_payload as (
select 
    CASE_ID,
    CASE_TYPE, 
    parse_json(
            '{ "create": false' || ', ' ||  
            '"case_type": "clinic",' || 
            '"case_id": ' || '"' || case_id || '", ' ||            
            '"external_id": ' || '"' || new_clinic_external_id || '", ' ||
            '"case_name": ' || '"' || CASE_NAME  || '",' || 
            '"properties": {' ||
                '"import_date": ' || '"' || replace(replace(replace(IMPORT_DATE::string, '"', '\\"'), '\n', '\\n'), '\r', '\\r') || '"' || 
            ' } }') payload 
    from clinic_case_update
)
,final as (
    select * from provider_case_update_payload
    union
    select * from clinic_case_update_payload
)
select * from final