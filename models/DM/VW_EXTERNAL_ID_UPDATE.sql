with 
dm_table_data_clinic as (
      select 
        replace(external_id, '_', '') as external_id_compare,
        case
        when case_id in ({{get_vcphcs_parent_case_id('child_value1')}})
        then '{{get_vcphcs_parent_case_id('parent_value1')}}'
        when case_id in ({{get_vcphcs_parent_case_id('child_value2')}})
        then '{{get_vcphcs_parent_case_id('parent_value2')}}'
        else parent_case_id end as new_parent_case_id,
        * 
      from {{ source('dm_table_data', 'CASE_CLINIC') }} where closed = 'FALSE' and external_id is not null
) 
,dm_table_data_provider as (
      select 
        replace(external_id, '_', '') as external_id_compare,
        * 
      from  {{ source('dm_table_data', 'CASE_PROVIDER') }} where closed = 'FALSE' and external_id is not null
)
-- BR: these clinics/providers are edge case clinics that do not meet the expectation of legacy and bhe clinics having different id's. 
,hades_table_exclude_bhe_in_legacy as ( 
    select 
        case
        when account_id in ('0014M00002QMGXuQAP', '0014M00002QMGXzQAP', '0014M00002QMGXkQAP', '0014M00002QMGYOQA5', '0014M00002QMGYEQA5', '0014M00002QMGYJQA5')
        then '0016100001H9ZZDAAB'
        else LEGACY_PARENT_ACCOUNT_ID end as LEGACY_PARENT_ACCOUNT_ID,
        LEGACY_ACCOUNT_ID,
        PARENT_ACCOUNT_ID,
        ACCOUNT_ID,
        BHA_GENERAL_ACCT,
        ACCOUNT_NAME,
        PROVIDER_DIRECTORY_DISPLAY_NAME,
        PROGRAM_NAME,
        ORIGINAL_DATE_OF_LICENSURE,
        SUD_LICENSE_NUMBER,
        CS_LICENSE_NUMBER,
        MH_DESIGNATION,
        RSSO_LICENSE_NUMBER,
        OFFERS_TELEHEALTH,
        PHONE,
        FAX,
        TDD_TTY,
        BILLING_ADDRESS_LINE_1,
        BILLING_CITY,
        COUNTY,
        BILLING_ZIP_POSTAL_CODE,
        BILLING_STATE_PROVINCE,
        HIDE_ADDRESS,
        PROVIDER_LOCATION_DISPLAY_LABEL,
        WEBSITE,
        ACTIVE_SUD_LICENSE,
        ACTIVE_MH_DESIGNATION,
        ACTIVE_RSSO_LICENSE,
        OPIOID_TREATMENT_PROGRAMS,
        RESIDENTIAL_CHILD_CARE_FACILITY,
        HOSPITAL,
        COMMUNITY_MENTAL_HEALTH_CENTER,
        COMMUNITY_MENTAL_HEALTH_CLINIC,
        PSYCHIATRIC_RESIDENTIAL,
        SUBSTANCE_USE_SERVICES,
        MENTAL_HEALTH_SETTINGS,
        RSSO_SERVICES_PROVIDED,
        POPULATION_SERVED,
        ACCESSIBILITY,
        NPI,
        FEES,
        LANGUAGES_SPOKEN,
        HOURS_OF_OPERATION_MONDAY,
        HOURS_OF_OPERATION_TUESDAY,
        HOURS_OF_OPERATION_WEDNESDAY,
        HOURS_OF_OPERATION_THURSDAY,
        HOURS_OF_OPERATION_FRIDAY,
        HOURS_OF_OPERATION_SATURDAY,
        HOURS_OF_OPERATION_SUNDAY,
        OUTPATIENT_SU_SERVICES,
        DAY_TREATMENT_PARTIAL_HOSPITALIZATION,
        INTENSIVE_OUTPATIENT_SU_SERVICES,
        CLINIC_MANAGED_LOW_INTENSE_RES_SVCS,
        CLINIC_MANAGED_MED_INTENSE_RES_SVCS,
        CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS,
        MEDICALLY_MONITORED_INTENSE_RES_TRTMT,
        CLINIC_MANAGED_RESIDENTIAL_DETOX,
        MED_MONITORED_INPATIENT_DETOX,
        TREATMENT_EVALUATION_72_HOUR,
        ACUTE_TREATMENT_UNIT,
        CRISIS_STABILIZATION_UNIT,
        DAY_TREATMENT,
        EMERGENCY,
        INTENSIVE_OUTPATIENT,
        OUTPATIENT,
        RESIDENTIAL_SHORT_TERM_TREATMENT,
        RESIDENTIAL_LONG_TERM_TREATMENT,
        CIRCLE_PROGRAM,
        MSO_AFFILIATION,
        RAE,
        ASO,
        ALCOHOL_DRUG_INVOLUNTARY_COMMITMENT,
        GENERAL_TREATMENT,
        DUI_DWI,
        YOUTH_TREATMENT,
        GENDER_RESPONSIVE_TTMT_FOR_WOMEN,
        EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS,
        PROVIDER_DIRECTORY_FORM_MODIFIED_DATE_MT,
        ACCEPTING_NEW_PATIENTS,
        TELEHEALTH_RESTRICTIONS,
        LATITUDE,
        LONGITUDE,
        SUD_EFFECTIVE_DATE,
        SUD_EXPIRATION_DATE,
        CS_EFFECTIVE_DATE,
        CS_EXPIRATION_DATE,
        MH_EFFECTIVE_DATE,
        MH_EXPIRATION_DATE,
        RSSO_EFFECTIVE_DATE,
        RSSO_EXPIRATION_DATE,
        ACTIVE_CS_LICENSE,
        BHE_LICENSE_NUMBER,
        ACTIVE_BHE_LICENSE,
        BHE_EFFECTIVE_DATE,
        BHE_EXPIRATION_DATE,
        DESIGNATION_NUMBER_2765,
        ACTIVE_2765_DESIGNATION,
        EFFECTIVE_DATE_2765,
        EXPIRATION_DATE_2765,
        SNE_APPROVAL_NUMBER,
        ACTIVE_SNE_APPROVAL,
        SNE_EFFECTIVE_DATE,
        SNE_EXPIRATION_DATE,
        SNC_APPROVAL_NUMBER,
        ACTIVE_SNC_APPROVAL,
        SNC_EFFECTIVE_DATE,
        SNC_EXPIRATION_DATE,
        FACILITY_TYPE,
        CSL_LICENSES,
        SERVICES_OFFERED,
        CRIMINAL_JUSTICE_SERVICES,
        EMERGENCY_AND_INVOLUNTARY_COMMITMENT_SER,
        RECOVERY_SUPPORTS,
        OUTPATIENT_EMERGENCY_CRISIS,
        MINOR_IN_POSSESSION,
        RESIDENTIAL_OVERNIGHT_EMERGENCY_CRISIS,
        OUTPATIENT_CERTIFICATION_TREATMENT,
        EMERGENCY_CERTIFICATION_INVOLUNTARY_CERT,
        CHILDREN_MINORS_UNDER_18_YEARS_OF_AGE,
        ADULTS_18_YEARS_OF_AGE_AND_OLDER,
        PRIORITY_POPULATIONS,
        PRIORITY_POP_SPECIALIZED_AGE_RANGE,
        BEHAVIORAL_HEALTH_INPATIENT_SERVICES,
        DRUG_SCHEDULES,
        DUI_DWAI_DRIVING_UNDER_THE_INFLUENCE_D,
        SERVICES_FOR_CHILDREN_AND_FAMILIES_SUD,
        SOAR_SUBSTANCE_USE_DISORDER_SUD_ED,
        SERVICES_FOR_CHILDREN_AND_FAMILIES_MH,
        BHE_WOMEN_AND_MATERNAL_BEHAVIORAL_SUD,
        BHE_EARLY_INTERVENTION_ASAM_LEVEL_0_5,
        BHE_OUTPATIENT_ASAM_LEVEL_1_TYPE,
        BHE_AMBULATORY_WITHDRAWAL_WITHOUT,
        BHE_INTENSIVE_OUTPATIENT_P_IOP_ASAM,
        BHE_AMBULATORY_WITHDRAWAL_WITH_EX,
        BHE_PARTIAL_HOSPITALIZATION_PHP_A,
        BHE_CLINICALLY_MONITORED,
        BHE_CLINICALLY_MANAGED,
        BHE_MEDICALLY_MONITORED,
        ACUTE_TREATMENT_UNIT_ATU_2765,
        CRISIS_STABILIZATION_UNIT_CSU_2765,
        SHORT_TERM_CERTIFICATION_TREATMENT_2765,
        LONG_TERM_CERTIFICATION_TREATMENT_2765,
        SNE_EMERGENCY_AND_CRISIS_B_HEALTH_S,
        SNE_EARLY_INTERVENTION_ASAM_LEVEL_0_5,
        SNE_OUTPATIENT_ASAM_LEVEL_1_TYPE,
        SNE_AMBULATORY_WITHDRAWAL_WITHOUT,
        SNE_INTENSIVE_OUTPATIENT_P_IOP_ASAM,
        SNE_PARTIAL_HOSPITALIZATION_PHP_ASAM,
        SNE_AMBULATORY_WITHDRAWAL_WITH_EX,
        SNE_CLINICALLY_MANAGED,
        SNE_CLINICALLY_MONITORED,
        SNE_MEDICALLY_MONITORED,
        SNE_INTEGRATED_CARE_SERVICES,
        SNC_EARLY_INTERVENTION_ASAM_LEVEL_0_5,
        SNC_OUTPATIENT_ASAM_LEVEL_1_TYPE,
        SNC_AMBULATORY_WITHDRAWAL_WITHOUT,
        SNC_INTENSIVE_OUTPATIENT_P_IOP_ASAM,
        SNC_PARTIAL_HOSPITALIZATION_PHP_A
    from 
        {{ source('hades_table_data', 'VWS_LADDERS_MAPPED_ACTIVE_LICENSES') }}
    where upper(account_id) not in 
        ( 
        '0014M00002QMJBGQAP',  --Young People in Recovery
        '0014M00002QMJ2CQAX', -- The Apprentice of Peace Youth Organization dba Trailhead Institute 
        '0014M00002QMIRYQA5', -- Advocates for Recovery Colorado
        '0014M00002QMI2YQAH', -- Built To Recover 
        '0014M00002QMKX0QAP' --Face It TOGETHER 
        )
)  
,hades_table_data_mapped_active_licenses_provider as (
      select 
        --replace(dm.external_id_format(legacy_parent_account_id), '_', '') || '-' || replace(dm.external_id_format(legacy_account_id), '_', '') as legacy_clinic_external_id,
        --dm.external_id_format(parent_account_id) || '-' || dm.external_id_format(account_id) as new_clinic_external_id,
        replace(dm.external_id_format(legacy_parent_account_id), '_', '') as legacy_provider_external_id,
        dm.external_id_format(parent_account_id) as new_provider_external_id,
        * from hades_table_exclude_bhe_in_legacy --{{ source('hades_table_data', 'VWS_LADDERS_MAPPED_ACTIVE_LICENSES') }}
      where legacy_parent_account_id is not null and parent_account_id is not null
)
,legacy_records_in_bhe_provider as (
    select * from hades_table_data_mapped_active_licenses_provider where parent_account_id=legacy_parent_account_id
)
,hades_table_data_mapped_active_licenses_clinic as (
      select 
        replace(dm.external_id_format(legacy_parent_account_id), '_', '') || '-' || replace(dm.external_id_format(legacy_account_id), '_', '') as legacy_clinic_external_id,
        dm.external_id_format(parent_account_id) || '-' || dm.external_id_format(account_id) as new_clinic_external_id,
        replace(dm.external_id_format(legacy_parent_account_id), '_', '') as legacy_provider_external_id,
        dm.external_id_format(parent_account_id) as new_provider_external_id,
        * from hades_table_exclude_bhe_in_legacy --{{ source('hades_table_data', 'VWS_LADDERS_MAPPED_ACTIVE_LICENSES') }}
      where legacy_parent_account_id is not null and legacy_account_id is not null
      and parent_account_id is not null and account_id is not null
)
,legacy_records_in_bhe_clinic as (
    select * from hades_table_data_mapped_active_licenses_clinic where parent_account_id=legacy_parent_account_id and account_id=legacy_account_id
)
-- not in legacy_records_in_bhe_provider
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
 from hades_table_data_mapped_active_licenses_provider bhe    
    inner join dm_table_data_provider on legacy_provider_external_id=dm_table_data_provider.external_id_compare
    where bhe.account_id not in (select account_id from legacy_records_in_bhe_provider)
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
-- not in legacy_records_in_bhe_clinic
-- legacy_clinic_external_id = clinic case's external_id
,clinic_case_update as (
select 
    legacy_clinic_external_id,
    new_clinic_external_id,
    dm_table_data_clinic.external_id_compare,
    dm_table_data_clinic.external_id,
    dm_table_data_clinic.case_id,
    dm_table_data_clinic.new_parent_case_id,
    dm_table_data_clinic.case_type,
    dm_table_data_clinic.case_name,
    current_timestamp() as import_date
 from hades_table_data_mapped_active_licenses_clinic bhe    
    inner join dm_table_data_clinic on legacy_clinic_external_id=dm_table_data_clinic.external_id_compare
    where bhe.account_id not in (select account_id from legacy_records_in_bhe_clinic)
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
            '"indices": ' || 
            '{ "parent": {' || 
                            '"case_id": ' || '"' || new_parent_case_id || '" } },' ||
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