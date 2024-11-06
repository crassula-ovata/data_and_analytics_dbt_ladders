with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_capacity as (
      select * from  {{ source('dm_table_data', 'CASE_CAPACITY') }}
), 

cte_check_property_update as 
(
select 
        capacity.case_id, 
        capacity.external_id,
        capacity.owner_id, 
        capacity.case_type,
        capacity.parent_relationship,
        capacity.parent_case_type,
        capacity.parent_case_id,
        -- clinic_phone_display
        case when nvl(capacity.clinic_phone_display, '') <> nvl(clinic.phone_display, '')
            then clinic.phone_display else null
            end as clinic_phone_display,
        -- clinic_referral_type_display
        case 
            when nvl(capacity.clinic_referral_type_display, '') = '' and nvl(clinic.referral_type, '') = '' then 'N/A'
            when nvl(clinic.referral_type, '') = '' and capacity.clinic_referral_type_display ='N/A' then null 
            when nvl(capacity.clinic_referral_type_display, '') <> nvl(clinic.referral_type, '') 
                then DM.FN_FORMAT_REFERRAL_TYPE(clinic.referral_type) else null 
            end as clinic_referral_type_display,
        -- clinic_case_name_display
        case when nvl(capacity.clinic_case_name_display, '') <> nvl(clinic.case_name, '') 
            then clinic.case_name else null 
            end as clinic_case_name_display,
        case when nvl(capacity.clinic_map_coordinates, '') <> nvl(clinic.map_coordinates, '') 
            then clinic.map_coordinates else null 
            end as clinic_map_coordinates,
         case  
            when 
            nvl(DM.GET_MAP_POPUP(clinic.display_name, clinic.phone_display, clinic.address_full, clinic.insurance, clinic.referral_type), '') <>  nvl(capacity.clinic_map_popup, '')
                then DM.GET_MAP_POPUP(clinic.display_name, clinic.phone_display, clinic.address_full, clinic.insurance, clinic.referral_type)
            else null end as clinic_map_popup,
        case 
            when nvl(DM.FN_GET_CLINIC_TYPE_OF_CARE_DISPLAY( clinic.residential_services,clinic.mental_health_settings), '') <> nvl(capacity.clinic_type_of_care_display, '') 
                then
               DM.FN_GET_CLINIC_TYPE_OF_CARE_DISPLAY( clinic.residential_services, clinic.mental_health_settings)
            else null 
            end as clinic_type_of_care_display, 
        case 
            when clinic.address_full is null and capacity.clinic_address_full_display is null then 'N/A'
            when clinic.address_full is null and (nvl(capacity.clinic_address_full_display, '') = 'N/A') then null 
            when nvl(clinic.address_full, '') <> nvl(capacity.clinic_address_full_display, '') then clinic.address_full 
            else null 
            end as clinic_address_full_display,
       case 
            when clinic.insurance is null and capacity.clinic_insurance_display is null then 'N/A'
            when clinic.insurance is null and nvl(capacity.clinic_insurance_display, '') = 'N/A' then null 
            when   nvl(DM.FN_INSURANCE_DISPLAY(clinic.insurance), '') <> nvl(capacity.clinic_insurance_display, '') 
                then DM.FN_INSURANCE_DISPLAY(clinic.insurance)
            else null end as clinic_insurance_display
    from  dm_table_data_capacity capacity 
    left join  dm_table_data_clinic clinic 
    on capacity.parent_case_id = clinic.case_id
    where capacity.closed = False
),
final as (
select * from cte_check_property_update 
where 
    clinic_phone_display is not null or
    clinic_referral_type_display is not null or
    clinic_case_name_display is not null or
    clinic_map_coordinates is not null or
    clinic_map_popup is not null or
    clinic_type_of_care_display is not null or
    clinic_address_full_display is not null or
    clinic_insurance_display is not null 
)
select 
    CASE_ID,
    EXTERNAL_ID,
    OWNER_ID,
    CASE_TYPE,
    PARENT_RELATIONSHIP,
    PARENT_CASE_TYPE,
    PARENT_CASE_ID,
    clinic_phone_display,
    clinic_referral_type_display,
    clinic_case_name_display,
    clinic_map_coordinates,
    clinic_map_popup,
    clinic_type_of_care_display,
    clinic_address_full_display,
    clinic_insurance_display
from final
