with 
external_id_from_payload as (
    select payload:case_type::string case_type,
        payload:case_id::string case_id,
        payload:external_id::string external_id
    from {{ ref('VW_EXTERNAL_ID_UPDATE')}}
)
,ladders_mapped_integration as (
    select * from {{ ref('VW_LADDERS_MAPPED_INTEGRATION')}}  
)
select * from ladders_mapped_integration 
    where exists (
        select external_id from external_id_from_payload where external_id=new_provider_external_id or external_id=new_clinic_external_id
    )
