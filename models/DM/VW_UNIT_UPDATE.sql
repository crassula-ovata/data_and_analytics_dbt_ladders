with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_unit as (
      select * from  {{ source('dm_table_data', 'CASE_UNIT') }}
), 

cte_check_property_update as 
(
select 
        -- 12/03: BR re-included clinic_display_name and related logic 
        unit.case_id, 
        unit.external_id,
        unit.owner_id, 
        unit.case_type,
        unit.parent_relationship,
        unit.parent_case_type,
        unit.parent_case_id,
        case when nvl(unit.map_coordinates, '') <> nvl(clinic.map_coordinates, '') 
            then clinic.map_coordinates else null end as map_coordinates,
        case 
            when clinic.display_name is null and unit.clinic_display_name is null then 'No information available'
            when clinic.display_name is null and nvl(unit.clinic_display_name, '') = 'No information available' then null 
            when nvl(clinic.display_name, '') <> nvl(unit.clinic_display_name, '') then clinic.display_name 
            else null end as clinic_display_name,
        -- 12/5 BR: included additional fields from BHA request
        case 
            when clinic.phone_display is null and unit.clinic_phone_display is null then 'No information available'
            when clinic.phone_display is null and nvl(unit.clinic_phone_display, '') = 'No information available' then null 
            when nvl(clinic.phone_display, '') <> nvl(unit.clinic_phone_display, '') then clinic.phone_display 
            else null end as clinic_phone_display,           
        case 
            when clinic.address_full is null and unit.clinic_address_full_display is null then 'No information available'
            when clinic.address_full is null and nvl(unit.clinic_address_full_display, '') = 'No information available' then null 
            when   nvl(clinic.address_full, '') <> nvl(unit.clinic_address_full_display, '') then clinic.address_full 
            else null end as clinic_address_full_display,
        case 
            when clinic.mental_health_settings is null and unit.clinic_mental_health_settings_display is null then 'No information available'
            when clinic.mental_health_settings is null and nvl(unit.clinic_mental_health_settings_display, '') = 'No information available' then null 
            when   nvl(DM.FN_MENTAL_HEALTH_SETTINGS_DISPLAY(clinic.mental_health_settings), '') <> nvl(unit.clinic_mental_health_settings_display, '') 
                then
                DM.FN_MENTAL_HEALTH_SETTINGS_DISPLAY(clinic.mental_health_settings)
            else null end as clinic_mental_health_settings_display,
        case 
            when clinic.insurance is null and unit.clinic_insurance_display is null then 'No information available'
            when clinic.insurance is null and nvl(unit.clinic_insurance_display, '') = 'No information available' then null 
            when   nvl(DM.FN_INSURANCE_DISPLAY(clinic.insurance), '') <> nvl(unit.clinic_insurance_display, '') 
                then DM.FN_INSURANCE_DISPLAY(clinic.insurance)
            else null end as clinic_insurance_display,
        case -- update clinic.referral_type), '') <> nvl([clinic - should be unit].clinic_map_popup, '') 
            when nvl(DM.GET_MAP_POPUP(clinic.display_name, clinic.phone_display, clinic.address_full, clinic.insurance, clinic.referral_type), '') <> nvl(unit.map_popup, '') 
                then DM.GET_MAP_POPUP(clinic.display_name, clinic.phone_display, clinic.address_full, clinic.insurance, clinic.referral_type)
            else null end as map_popup
    from dm_table_data_unit unit 
    left join dm_table_data_clinic clinic
    on unit.parent_case_id = clinic.case_id
    where unit.closed = False
),
final as (
select * from cte_check_property_update 
where 
    map_coordinates is not null or
    clinic_display_name is not null or
    clinic_phone_display is not null or
    clinic_address_full_display is not null or
    clinic_mental_health_settings_display is not null or
    clinic_insurance_display is not null or
    map_popup is not null
)

select 
	CASE_ID,
	EXTERNAL_ID,
	OWNER_ID,
	CASE_TYPE,
	PARENT_RELATIONSHIP,
	PARENT_CASE_TYPE,
	PARENT_CASE_ID,
	MAP_COORDINATES,
	CLINIC_DISPLAY_NAME,
	CLINIC_PHONE_DISPLAY,
	CLINIC_ADDRESS_FULL_DISPLAY,
	CLINIC_MENTAL_HEALTH_SETTINGS_DISPLAY,
	CLINIC_INSURANCE_DISPLAY,
	MAP_POPUP
from final