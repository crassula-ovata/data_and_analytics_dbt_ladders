with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
hades_table_data_ladders_active_licenses as (
      --select * from  {{ source('hades_table_data', 'VWS_LADDERS_ACTIVE_LICENSES') }}
      select *,
      case  
            when nvl(legacy_account_id, '') <> account_id then 'yes'
            when  upper(account_id) in 
                ( 
                    '0014M00002QMJBGQAP',  --Young People in Recovery
                    '0014M00002QMJ2CQAX', -- The Apprentice of Peace Youth Organization dba Trailhead Institute 
                    '0014M00002QMIRYQA5', -- Advocates for Recovery Colorado
                    '0014M00002QMI2YQAH', -- Built To Recover 
                    '0014M00002QMKX0QAP' --Face It TOGETHER
                    '0014M00002KIXB3QAB'  --Milestone Community Wellness, LLC 
                ) then 'yes'
        else null end as bhe_updated
     from DM.VW_LADDERS_MAPPED_INTEGRATION_TABLE
), 
locs as (
      select * from  {{ source('dm_table_data', 'LOCATION') }}
),
c_prod as (
    -- get the oldest clinic record by date_oponed for a given external_id
    -- 4/10: KC added county
    -- 5/31: KC ran this sub-select to find that the CommCare properties were not created, Anthony created this see ticket
    -- 6/1: KC checked sub-select to find that Commcare properties did not make it to Snowflake AWS, assigned to Shu to take a look
    select * from (
        select case_id, external_id, owner_id, case_name, display_name, account_name, county, address_city, address_full, 
            address_state, address_street, phone_number, phone_details, phone_display as phone_display, address_zip, clinic_type, substance_use_services, opioid_treatment_provider,
            --5/28 sprint D: BR include new fields
            original_licensure_date, 
            rsso_license_number, offers_telehealth, fax_number, tdd_tty, provider_location_display_label,  website, npi,
            monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours, saturday_hours, sunday_hours, hide_address,
            -- circle_program, 
            case when circle_program = 'TRUE' then 'yes' 
                when circle_program = 'FALSE' then 'no' 
                else null end as circle_program,
            mso_affiliation, rae, aso, 
            provider_directory_form_modified_date, 
            -- acceptingƒ_new_patients,
            case when accepting_new_patients = TRUE then 'yes' else null end as accepting_new_patients,
            telehealth_restrictions, service_types,
            sud_license_number, cs_license_number,
            mh_designation, 
            accessibility,
            population_served, 
            gender,
            insurance, language_services, monday_open, monday_close, tuesday_open, tuesday_close,
            wednesday_open, wednesday_close, thursday_open, thursday_close, friday_open, friday_close, saturday_open, saturday_close,
            sunday_open, sunday_close, mental_health_settings,
            residential_services, latitude,longitude,
             -- 6/15 BR updates to include map_coordintes
             map_coordinates,
            -- KC: end of new properties.
            -- 8/20/23 additional 2 fields for map_popup
            referral_type,
            case when bhe_updated = TRUE then 'yes' else null end as bhe_updated,
            map_popup,
            -- 12/1 for tile_header | 12/4 BR: commented out tile_header related
            -- exclusions,
            date_opened, parent_case_id, parent_case_type, parent_relationship, ACTIVE_SUD_LICENSE, ACTIVE_MH_DESIGNATION,
            rank () over ( partition by external_id order by date_opened asc ) as date_rank_c 
        from dm_table_data_clinic where closed = 'FALSE') 
    where date_rank_c = 1
    
),
p_prod as (
     -- per woody and kirti, get the oldest provider record by date_opened for given external_id
     select * from (
         select case_id, external_id, date_opened, 
            rank () over ( partition by external_id order by date_opened asc ) as date_rank_p        
         from dm_table_data_provider p1 where closed = 'FALSE' and external_id is not null) 
     where date_rank_p = 1 
),

c_share as ( 
    select 
        dm.external_id_format(parent_account_id) || '-' || dm.external_id_format(account_id) as ladders_external_id,
        dm.external_id_format(parent_account_id) as provider_external_id,
        bha_general_acct as provider_name,
        case when provider_directory_display_name <> '' then replace(replace(replace(provider_directory_display_name, '?', '-'), char(13)), char(10))
             when program_name <>'' then replace(replace(replace(program_name, '?', '-'), char(13)), char(10))
             else account_name 
        end as case_name, 
        -- 6/14 removing "?"
        case when provider_directory_display_name <> '' then replace(replace(replace(provider_directory_display_name, '?', '-'), char(13)), char(10))
             when program_name <>'' then replace(replace(replace(program_name, '?', '-'), char(13)), char(10))
             else account_name 
        end as display_name,
        account_name as account_name,
        -- 4/10: include county from BHA LADDERS
        initcap(county, ' ') as county,
        initcap(billing_city, ' ') as address_city,
        case
            when billing_address_line_1 ='' and billing_city <> '' and billing_zip_postal_code <> ''
                then initcap(billing_city, ' ') || ', ' || 'CO' || ' ' || billing_zip_postal_code
            when billing_address_line_1 ='' and billing_city = '' and billing_zip_postal_code <> ''
                then 'CO' || ' ' || billing_zip_postal_code
            when billing_address_line_1 <> '' and billing_city = '' and billing_zip_postal_code = ''
                then replace(replace(billing_address_line_1, char(13)), char(10))
            when billing_address_line_1 = '' and billing_city = '' and billing_address_line_1 = '' 
                then null 
            else  replace(replace(billing_address_line_1, char(13)), char(10)) || ', ' || initcap(billing_city, ' ') || ', ' || 'CO' || ' ' || billing_zip_postal_code
        end as address_full,
        iff(billing_state_province = '', null,'CO') as address_state,
        --billing_address_line_1 as address_street,
        --replace(billing_address_line_1, '\n', ' ') as address_street,
        replace(replace(billing_address_line_1, char(13)), char(10)) as address_street,
        billing_zip_postal_code as address_zip,            
        iff(phone = '', null, '1' || substr(regexp_replace(phone, '[^a-zA-Z0-9]'), 1,10)) as phone,
        case when  startswith(trim(substr(phone, 15), ' '), '/') then replace(trim(substr(phone, 15), ' '), '/', '')
             else trim(substr(phone, 15), ' ')  end as phone_details,
        iff(phone = '', null, '1' || substr(regexp_replace(phone, '[^a-zA-Z0-9]'), 1,10)) as phone_number, iff(phone = '', null,  concat('(', substr(regexp_replace(phone, '[^a-zA-Z0-9]'), 1,3), ') ', substr(regexp_replace(phone, '[^a-zA-Z0-9]'),  4,3), '-', substr(regexp_replace(phone, '[^a-zA-Z0-9]'), 7,4) )) as phone_display, 
        ifnull(opioid_treatment_programs, false) as opioid_treatment_provider,
        residential_child_care_facility,
        hospital,
        community_mental_health_center,
        community_mental_health_clinic,
        
        -- case when coalesce(legacy_account_id, '') <> coalesce(account_id, '') then
        case when bhe_updated is not null and bhe_updated = 'yes' then
            dm.fn_get_substance_use_services(
                    OUTPATIENT,
                    INTENSIVE_OUTPATIENT_SU_SERVICES,
                    GENERAL_TREATMENT,
                    DUI_DWI,
                    EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS,
                    CLINIC_MANAGED_LOW_INTENSE_RES_SVCS,
                    CLINIC_MANAGED_MED_INTENSE_RES_SVCS,
                    CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS,
                    CLINIC_MANAGED_RESIDENTIAL_DETOX,
                    DUI_DWAI_DRIVING_UNDER_THE_INFLUENCE_D,
                    GENDER_RESPONSIVE_TTMT_FOR_WOMEN,
                    DAY_TREATMENT_PARTIAL_HOSPITALIZATION,
                    MED_MONITORED_INPATIENT_DETOX,
                    MEDICALLY_MONITORED_INTENSE_RES_TRTMT
            ) else lower(replace(replace(substance_use_services, ' ','_'), ';_', ' ')) 
            end as substance_use_services,
        -- JOE 12/21/23: This is to exclude general_treatment, intensive_outpatient, and outpatient
        -- case when coalesce(legacy_account_id, '') <> coalesce(account_id, '') then
         case when bhe_updated is not null and bhe_updated = 'yes' then
           fn_get_residential_services(
                DUI_DWI,
                EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS,
                CLINIC_MANAGED_LOW_INTENSE_RES_SVCS,
                CLINIC_MANAGED_MED_INTENSE_RES_SVCS,
                CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS,
                CLINIC_MANAGED_RESIDENTIAL_DETOX,
                DUI_DWAI_DRIVING_UNDER_THE_INFLUENCE_D,
                GENDER_RESPONSIVE_TTMT_FOR_WOMEN,
                DAY_TREATMENT_PARTIAL_HOSPITALIZATION,
                MED_MONITORED_INPATIENT_DETOX,
                MEDICALLY_MONITORED_INTENSE_RES_TRTMT
            )
        else 
        trim(
            regexp_replace(
                replace(
                    replace(
                        replace(
                            replace(
                                regexp_replace(
                                    replace(
                                        regexp_replace(
                                            replace(
                                                lower(substance_use_services),
                                                '&',
                                                'and'
                                            ),
                                            '[()]',
                                            ''
                                        ),
                                        ';\\s',
                                        ';'
                                    ),
                                    '[^a-zA-Z0-9;]+', '_'
                                ),
                                ';_',
                                ' '
                            ),
                            'general_treatment',
                            ''
                        ),
                        'intensive_outpatient',
                        ''
                    ),
                    'outpatient',
                    ''
                ),
                ' +',
                ' '
            )
        ) end as residential_services,
        outpatient_su_services,
        intensive_outpatient_su_services,
        clinic_managed_low_intense_res_svcs,
        clinic_managed_med_intense_res_svcs,
        clinic_managed_high_intense_res_svcs,
        medically_monitored_intense_res_trtmt,
        clinic_managed_residential_detox,
        med_monitored_inpatient_detox,
       --5/24 sprint D: BR include new fields
        original_date_of_licensure as original_licensure_date,
        sud_license_number,
        cs_license_number,
        mh_designation,
        rsso_license_number,
        replace(replace(lower(offers_telehealth), '- ', ''), ' ', '_') as offers_telehealth,
        -- 6/6 BR - Update fax_number and tdd_tty to handle na values and string that aren't valid values
         case when regexp_like((iff(fax = '', null, '1' || substr(regexp_replace(fax, '[^a-zA-Z0-9]'), 1,10))), '[0-9]*') then ('1' || substr(regexp_replace(fax, '[^a-zA-Z0-9]'), 1,10))
         else null end as fax_number,
        case when regexp_like((iff(tdd_tty = '', null, '1' || substr(regexp_replace(tdd_tty, '[^a-zA-Z0-9]'), 1,10))), '[0-9]*') then ('1' || substr(regexp_replace(tdd_tty, '[^a-zA-Z0-9]'), 1,10))
         else null end as tdd_tty,
        hide_address,
        replace(replace(provider_location_display_label, '<br>', ' '), '_BR_ENCODED_', ' ' ) as provider_location_display_label,
        -- BR: 12/13 - Updated to check for valid website link
        case
          when startswith(upper(website), 'HTTP://') or startswith(upper(website), 'HTTPS://') THEN website
          when startswith(upper(website), 'WWW.') then 'https://' || website
          else 'https://www.' ||  nullif(trim(website), '')
        end as website,
        -- replace(regexp_replace(replace(regexp_replace(lower(population_served),  ';\\s', ';'), '+', ''), '[^a-zA-Z0-9;]', '_'), ';', ' ') as population_served,
        -- 6/21: BR updated to map youth-> minors_adolescents and only keep one if both youth and minor/adolescets exist
        -- case 
    --     when (contains(population_served, 'Youth') and contains(population_served, 'Minors/Adolescents')) then 
    -- replace(regexp_replace(replace(regexp_replace(regexp_replace(lower(population_served),  ';\\s', ';'), 'youth;|; youth', ''), '+', ''), '[^a-zA-Z0-9;]', '_'), ';', ' ') 
    --     else replace(replace(regexp_replace(replace(regexp_replace(lower(population_served),  ';\\s', ';'), '+', ''), '[^a-zA-Z0-9;]', '_'), ';', ' '), 'youth', 'minors_adolescents') end as population_served,
        case when nvl(population_served, '') <> '' then 
        dm.extract_from_population_served(population_served, false)  else '' end as population_served,
        case when nvl(population_served, '') <> '' then 
        dm.extract_from_population_served(population_served, true) else '' end as gender,
        replace(regexp_replace(replace(lower(accessibility),  ';\\s', ';'), '[^a-zA-Z0-9;]', '_'), ';_', ' ') as accessibility,
        replace(regexp_replace(replace(lower(fees),  ';\\s', ';'), '[^a-zA-Z0-9;]', '_'), ';_', ' ') as insurance,
            -- JOE 12/21/23: This is to format asl properly
        replace(
            regexp_replace(
                replace(
                    replace(
                        lower(languages_spoken),
                        '&',
                        'and'
                    ),
                    ';\\s',
                    ';'
                ),
                '[^a-zA-Z0-9;()]',
                '_'
            ),
            ';_',
            ' '
        ) as language_services,
        case 
            when regexp_replace(npi, '[^0-9]', '') = npi then npi
            when lower(npi) = 'nonpifound' then 'no_npi_found' 
            else '' end as npi,
        hours_of_operation_monday as monday_hours,
        trim(split_part(hours_of_operation_monday, '-', 1)) as monday_open,
        trim(split_part(hours_of_operation_monday, '-', 2)) as monday_close,
        hours_of_operation_tuesday as tuesday_hours,
        trim(split_part(hours_of_operation_tuesday, '-', 1)) as tuesday_open,
        trim(split_part(hours_of_operation_tuesday, '-', 2)) as tuesday_close,
        hours_of_operation_wednesday as wednesday_hours,
        trim(split_part(hours_of_operation_wednesday, '-', 1)) as wednesday_open,
        trim(split_part(hours_of_operation_wednesday, '-', 2)) as wednesday_close,
        hours_of_operation_thursday as thursday_hours,
        trim(split_part(hours_of_operation_thursday, '-', 1)) as thursday_open,
        trim(split_part(hours_of_operation_thursday, '-', 2)) as thursday_close,
        hours_of_operation_friday as friday_hours,
        trim(split_part(hours_of_operation_friday, '-', 1)) as friday_open,
        trim(split_part(hours_of_operation_friday, '-', 2)) as friday_close,
        hours_of_operation_saturday as saturday_hours,
        trim(split_part(hours_of_operation_saturday, '-', 1)) as saturday_open,
        trim(split_part(hours_of_operation_saturday, '-', 2)) as saturday_close,
        hours_of_operation_sunday as sunday_hours,
        trim(split_part(hours_of_operation_sunday, '-', 1)) as sunday_open,
        trim(split_part(hours_of_operation_sunday, '-', 2)) as sunday_close,
        lower(circle_program) as circle_program,
        mso_affiliation,
        replace(rae, ';', ' ') as rae,
        replace(lower(replace(regexp_replace(aso, '[: ]', '_'), ';_', ' ')), '__', '_') as aso,
        -- -- 5/31: KC updated added "to_date" to return only YYYY-MM-DD date, removing the time component
        to_date(to_timestamp(PROVIDER_DIRECTORY_FORM_MODIFIED_DATE_MT, 'MM/DD/YYYY HH12:MI AM')) as provider_directory_form_modified_date, 
        accepting_new_patients,
        telehealth_restrictions,
        dm.fn_validate_lat(latitude) as latitude,
        dm.fn_validate_long(longitude) as longitude,
        -- 6/15 BR updates to include map_coordintes
        concat(dm.fn_validate_lat(latitude), ' ', dm.fn_validate_long(longitude)) as map_coordinates,
        -- JOE 12/21/23: This is to exclude intensive_outpatient and outpatient

        -- case when coalesce(legacy_account_id, '') <> coalesce(account_id, '') then
         case when bhe_updated is not null and bhe_updated = 'yes' then
            dm.fn_get_mental_health_settings(
                hospital, emergency, treatment_evaluation_72_hour, residential_long_term_treatment,
                residential_short_term_treatment, residential_child_care_facility, crisis_stabilization_unit,
                acute_treatment_unit
            )
        else 
        trim(
            regexp_replace(
                replace(
                    replace(
                        replace(
                            regexp_replace(
                                replace(
                                    replace(
                                        lower(mental_health_settings),
                                        '&',
                                        'and'
                                    ),
                                    ';\\s',
                                    ';'
                                ),
                                '[^a-zA-Z0-9;]',
                                '_'
                            ),
                            ';_',
                            ' '
                        ),
                        'intensive_outpatient',
                        ''
                    ),
                    'outpatient',
                    ''
                ),
                ' +',
                ' '
            )
        ) 
        end as mental_health_settings,
        -- properties used to calculate mental_health_settings_list and the new field mental_health_settings 6/23: BR mental_health_settings_list not used anymore. mapped from mental_health_settings
        psychiatric_residential,
        TREATMENT_EVALUATION_72_HOUR,
        ACUTE_TREATMENT_UNIT,
        CRISIS_STABILIZATION_UNIT,
        DAY_TREATMENT,
        EMERGENCY,
        INTENSIVE_OUTPATIENT,
        OUTPATIENT,
        RESIDENTIAL_SHORT_TERM_TREATMENT,
        RESIDENTIAL_LONG_TERM_TREATMENT,
        -- properties used to calculate residential_services_list and the new field residential_services 6/23: BR residential_services_list not used anymore. mapped from substance_user_services
        EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS,
        GENDER_RESPONSIVE_TTMT_FOR_WOMEN,
        YOUTH_TREATMENT,
        DUI_DWI,
        GENERAL_TREATMENT,
        RSSO_SERVICES_PROVIDED,
        ALCOHOL_DRUG_INVOLUNTARY_COMMITMENT,
        -- properties used to calculate service_types_list and the new field service_types
        ACTIVE_SUD_LICENSE,
        ACTIVE_MH_DESIGNATION,
        ACTIVE_RSSO_LICENSE,
        BHE_UPDATED
    from hades_table_data_ladders_active_licenses 
    order by case_name
), 

clinic_type_list as (
    select 
        ladders_external_id,
        case 
            when hospital = 'TRUE' then 'hospital' else null end as hospital,
        case 
            when community_mental_health_center = 'TRUE' then 'community_mental_health_center' else null end as community_mental_health_center,
        case 
            when community_mental_health_clinic = 'TRUE' then 'community_mental_health_clinic' else null end as community_mental_health_clinic,
        case 
            when residential_child_care_facility = 'TRUE' then 'residential_child_care_facility' else null end as residential_child_care_facility,
        case 
            when opioid_treatment_provider = 'TRUE' then 'opioid_treatment_provider' else null end as opioid_treatment_provider,
        case 
             when 
             (
                outpatient_su_services is not null or 
                intensive_outpatient_su_services is not null or 
                substance_use_services is not null or
                clinic_managed_low_intense_res_svcs = 'TRUE' or
                clinic_managed_med_intense_res_svcs = 'TRUE' or
                clinic_managed_high_intense_res_svcs = 'TRUE' or
                medically_monitored_intense_res_trtmt = 'TRUE' or
                clinic_managed_residential_detox = 'TRUE' or
                med_monitored_inpatient_detox = 'TRUE'
             ) then 'substance_use_services' 
             else null  end as substance_use_services
    from c_share 
    order by ladders_external_id),

clinic_type_prod as (
    select 
        external_id, 
        clinic_type
    from c_prod
)
, clinic_type_share as (
   select ladders_external_id, listagg (services, ' ') WITHIN GROUP(order by services) as clinic_type 
   from 
   (
       select ladders_external_id, services 
       from clinic_type_list
            unpivot (services for column_list in (hospital,
                                        community_mental_health_center, 
                                        community_mental_health_clinic,
                                        residential_child_care_facility,
                                        opioid_treatment_provider,
                                        substance_use_services)
                    )
    ) group by ladders_external_id order by ladders_external_id desc
)

, clinic_type as (
	select *
	from (
		select *, 1 as priority from clinic_type_share
		union
		select *, 2 as priority from clinic_type_prod
	)
	qualify row_number() over (partition by LADDERS_EXTERNAL_ID order by priority asc) = 1
)

, service_types_list as (
    select ladders_external_id, 
        case when ACTIVE_SUD_LICENSE = 'TRUE' then 'substance_use' else null end as substance_use,
        case when ACTIVE_MH_DESIGNATION = 'TRUE' then 'mental_health' else null end as mental_health,
        case when ACTIVE_RSSO_LICENSE = 'TRUE' then 'recovery_support_services_organization' else null end as recovery_support_services_organization
    from c_share 
    order by ladders_external_id
),

service_types_share as (
select ladders_external_id, listagg (services, ' ') WITHIN GROUP(order by services) as service_types 
   from 
   (
       select ladders_external_id, services 
       from service_types_list
            unpivot (services for column_list in (substance_use,mental_health, recovery_support_services_organization))
    ) group by ladders_external_id order by ladders_external_id desc
), 

service_types_prod as (
    select external_id, service_types 
   from c_prod
)
, service_types as (
	select *
	from (
		select *, 1 as priority from service_types_share
		union
		select *, 2 as priority from service_types_prod
	)
	qualify row_number() over (partition by LADDERS_EXTERNAL_ID order by priority asc) = 1
)
, map_popup_cte_share as (
select ladders_external_id, c_share.display_name, c_share.phone_display, c_share.address_full, c_share.insurance, c_prod.referral_type,
dm.get_map_popup(c_share.display_name, c_share.phone_display, c_share.address_full, c_share.insurance, c_prod.referral_type) as map_popup
 from  c_share join c_prod on c_share.ladders_external_id = c_prod.external_id
    order by ladders_external_id
)
, map_popup_cte_prod as (
select external_id, display_name, phone_display, address_full, insurance, referral_type, map_popup
 from c_prod
)
, map_popup_cte as (
	select *
	from (
		select *, 1 as priority from map_popup_cte_share
		union
		select *, 2 as priority from map_popup_cte_prod
	)
	qualify row_number() over (partition by LADDERS_EXTERNAL_ID order by priority asc) = 1
)
-- 05/06/2024 c_prod_prep (along with c_share_union) will be used for additional data check between c_share and c_prod (with c_share is the primary data source)
-- that if a case is not synced between c_share and c_prod, data will be retained between these two data sources
,c_prod_prep as (
	select 
	EXTERNAL_ID as LADDERS_EXTERNAL_ID
	,split_part(EXTERNAL_ID, '-', 1) as PROVIDER_EXTERNAL_ID
	,null as PROVIDER_NAME
	, CASE_NAME
	, DISPLAY_NAME
	, ACCOUNT_NAME
	, COUNTY
	, ADDRESS_CITY
	, ADDRESS_FULL
	, ADDRESS_STATE
	, ADDRESS_STREET
	, ADDRESS_ZIP
	,PHONE_NUMBER as PHONE
	, PHONE_DETAILS
	, PHONE_NUMBER
	, PHONE_DISPLAY
	, OPIOID_TREATMENT_PROVIDER
	,null as RESIDENTIAL_CHILD_CARE_FACILITY
	,null as HOSPITAL
	,null as COMMUNITY_MENTAL_HEALTH_CENTER
	,null as COMMUNITY_MENTAL_HEALTH_CLINIC
	, SUBSTANCE_USE_SERVICES
	, RESIDENTIAL_SERVICES
	,null as OUTPATIENT_SU_SERVICES
	,null as INTENSIVE_OUTPATIENT_SU_SERVICES
	,null as CLINIC_MANAGED_LOW_INTENSE_RES_SVCS
	,null as CLINIC_MANAGED_MED_INTENSE_RES_SVCS
	,null as CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS
	,null as MEDICALLY_MONITORED_INTENSE_RES_TRTMT
	,null as CLINIC_MANAGED_RESIDENTIAL_DETOX
	,null as MED_MONITORED_INPATIENT_DETOX
	, ORIGINAL_LICENSURE_DATE
	, SUD_LICENSE_NUMBER
	, CS_LICENSE_NUMBER
	, MH_DESIGNATION
	, RSSO_LICENSE_NUMBER
	, OFFERS_TELEHEALTH
	, FAX_NUMBER
	, TDD_TTY
	, HIDE_ADDRESS
	, PROVIDER_LOCATION_DISPLAY_LABEL
	, WEBSITE
	, POPULATION_SERVED
	, GENDER
	, ACCESSIBILITY
	, INSURANCE
	, LANGUAGE_SERVICES
	, NPI
	, MONDAY_HOURS
	, MONDAY_OPEN
	, MONDAY_CLOSE
	, TUESDAY_HOURS
	, TUESDAY_OPEN
	, TUESDAY_CLOSE
	, WEDNESDAY_HOURS
	, WEDNESDAY_OPEN
	, WEDNESDAY_CLOSE
	, THURSDAY_HOURS
	, THURSDAY_OPEN
	, THURSDAY_CLOSE
	, FRIDAY_HOURS
	, FRIDAY_OPEN
	, FRIDAY_CLOSE
	, SATURDAY_HOURS
	, SATURDAY_OPEN
	, SATURDAY_CLOSE
	, SUNDAY_HOURS
	, SUNDAY_OPEN
	, SUNDAY_CLOSE
	, CIRCLE_PROGRAM
	, MSO_AFFILIATION
	, RAE
	, ASO
	, PROVIDER_DIRECTORY_FORM_MODIFIED_DATE
	, ACCEPTING_NEW_PATIENTS
	, TELEHEALTH_RESTRICTIONS
	, LATITUDE
	, LONGITUDE
	, MAP_COORDINATES
	, MENTAL_HEALTH_SETTINGS
	,null as PSYCHIATRIC_RESIDENTIAL
	,null as TREATMENT_EVALUATION_72_HOUR
	,null as ACUTE_TREATMENT_UNIT
	,null as CRISIS_STABILIZATION_UNIT
	,null as DAY_TREATMENT
	,null as EMERGENCY
	,null as INTENSIVE_OUTPATIENT
	,null as OUTPATIENT
	,null as RESIDENTIAL_SHORT_TERM_TREATMENT
	,null as RESIDENTIAL_LONG_TERM_TREATMENT
	,null as EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS
	,null as GENDER_RESPONSIVE_TTMT_FOR_WOMEN
	,null as YOUTH_TREATMENT
	,null as DUI_DWI
	,null as GENERAL_TREATMENT
	,null as RSSO_SERVICES_PROVIDED
	,null as ALCOHOL_DRUG_INVOLUNTARY_COMMITMENT
	, ACTIVE_SUD_LICENSE
	, ACTIVE_MH_DESIGNATION
	,null as ACTIVE_RSSO_LICENSE,
    BHE_UPDATED
	from c_prod
)
, c_share_union as (
	select *
	from (
		select *, 1 as priority from c_share
		union
		select *, 2 as priority from c_prod_prep
	)
	qualify row_number() over (partition by LADDERS_EXTERNAL_ID order by priority asc) = 1
)
, final as (
select
        c_prod.external_id as prod_external_id,   -- from prod
        c_prod.case_id as prod_case_id, -- from prod
        c_prod.date_opened as prod_date_opened, -- from prod
        c_share_union.ladders_external_id,
        locs.location_id as owner_id,        
        'clinic' as case_type,
        'extension' as parent_relationship, 
        'provider' as parent_case_type,
        case 
            when c_prod.parent_case_id is not null then c_prod.parent_case_id
            when c_prod.parent_case_id is null and p_prod.case_id is not null then p_prod.case_id
            else null 
            end as parent_case_id,            
        c_share_union.provider_external_id,
        iff(c_share_union.case_name = '', null,c_share_union.case_name) as case_name, 
        iff(c_share_union.display_name ='', null, c_share_union.display_name) as display_name,
        iff(c_share_union.account_name ='', null, c_share_union.account_name) as account_name,
        -- 4/10: Include county check
        iff(c_share_union.county is null, '', c_share_union.county) as county,
        iff(c_share_union.address_city is null, '', c_share_union.address_city) as address_city,
        iff(c_share_union.address_full is null, '', c_share_union.address_full) as address_full,
        iff(c_share_union.address_state is null, '', c_share_union.address_state) as address_state,
        iff(c_share_union.address_street is null, '', c_share_union.address_street) as address_street,
        iff(c_share_union.address_zip is null, '', c_share_union.address_zip) as address_zip,    
        iff(clinic_type.clinic_type is null, '', clinic_type.clinic_type) as clinic_type,
        iff(c_share_union.phone is null, '', c_share_union.phone) as phone,
        iff(c_share_union.phone_details is null, '', c_share_union.phone_details) as phone_details,
        iff(c_share_union.phone_display is null, '', c_share_union.phone_display) as phone_display,
        --5/28 sprint D: BR include new fields
        c_share_union.original_licensure_date as original_licensure_date,
        iff(c_share_union.sud_license_number is null, '', c_share_union.sud_license_number) as sud_license_number,
        iff(c_share_union.cs_license_number is null, '', c_share_union.cs_license_number) as cs_license_number,
        iff(c_share_union.mh_designation is null, '', c_share_union.mh_designation) as mh_designation,
        iff(c_share_union.rsso_license_number is null, '', c_share_union.rsso_license_number) as rsso_license_number,
        iff(c_share_union.offers_telehealth is null, '', c_share_union.offers_telehealth) as offers_telehealth,
        iff(length(iff(c_share_union.fax_number is null, '', c_share_union.fax_number)) >=10, c_share_union.fax_number, null) as fax_number,
        iff(length(iff(c_share_union.tdd_tty is null, '', c_share_union.tdd_tty)) >=10, c_share_union.tdd_tty, null) as tdd_tty_calc,
        iff(length(iff(c_share_union.tdd_tty is null, '', c_share_union.tdd_tty)) >=10, c_share_union.tdd_tty, null) as tdd_tty,
        iff(c_share_union.provider_location_display_label is null, '', c_share_union.provider_location_display_label) as provider_location_display_label,
        iff(c_share_union.website is null, '', c_share_union.website) as website,
        iff(c_share_union.population_served is null, '', c_share_union.population_served) as population_served,
        iff(c_share_union.gender is null, '', c_share_union.gender) as gender,
        iff(c_share_union.accessibility is null, '', c_share_union.accessibility) as accessibility,
        iff(c_share_union.insurance is null, '', c_share_union.insurance) as insurance,
        iff(c_share_union.language_services is null, '', c_share_union.language_services) as language_services,
        iff(c_share_union.npi is null, '', c_share_union.npi) as npi,
        iff(c_share_union.monday_hours is null, '', c_share_union.monday_hours) as monday_hours,
        iff(c_share_union.monday_open is null, '', c_share_union.monday_open) as monday_open,
        iff(c_share_union.monday_close is null, '', c_share_union.monday_close) as monday_close,
        iff(c_share_union.tuesday_hours is null, '', c_share_union.tuesday_hours) as tuesday_hours,
        iff(c_share_union.tuesday_open is null, '', c_share_union.tuesday_open) as tuesday_open,
        iff(c_share_union.tuesday_close is null, '', c_share_union.tuesday_close) as tuesday_close,
        iff(c_share_union.wednesday_hours is null, '', c_share_union.wednesday_hours) as wednesday_hours,
        iff(c_share_union.wednesday_open is null, '', c_share_union.wednesday_open) as wednesday_open,
        iff(c_share_union.wednesday_close is null, '', c_share_union.wednesday_close) as wednesday_close,
        iff(c_share_union.thursday_hours is null, '', c_share_union.thursday_hours) as thursday_hours,
        iff(c_share_union.thursday_open is null, '', c_share_union.thursday_open) as thursday_open,
        iff(c_share_union.thursday_close is null, '', c_share_union.thursday_close) as thursday_close, 
        iff(c_share_union.friday_hours is null, '', c_share_union.friday_hours) as friday_hours,
        iff(c_share_union.friday_open is null, '', c_share_union.friday_open) as friday_open,
        iff(c_share_union.friday_close is null, '', c_share_union.friday_close) as friday_close,
        iff(c_share_union.saturday_hours is null, '', c_share_union.saturday_hours) as saturday_hours,
        iff(c_share_union.saturday_open is null, '', c_share_union.saturday_open) as saturday_open,
        iff(c_share_union.saturday_close is null, '', c_share_union.saturday_close) as saturday_close,
        iff(c_share_union.sunday_hours is null, '', c_share_union.sunday_hours) as sunday_hours,
        iff(c_share_union.sunday_open is null, '', c_share_union.sunday_open) as sunday_open,
        iff(c_share_union.sunday_close is null, '', c_share_union.sunday_close) as sunday_close,
        iff(c_share_union.circle_program is null, '', c_share_union.circle_program) as circle_program,
        iff(c_share_union.mso_affiliation is null, '', c_share_union.mso_affiliation) as mso_affiliation,
        iff(c_share_union.rae is null, '', c_share_union.rae) as rae,
        iff(c_share_union.aso is null, '', c_share_union.aso) as aso,
        c_share_union.provider_directory_form_modified_date as provider_directory_form_modified_date,
        iff(c_share_union.accepting_new_patients is null, '', c_share_union.accepting_new_patients) as accepting_new_patients,
        iff(c_share_union.telehealth_restrictions is null, '', c_share_union.telehealth_restrictions) as telehealth_restrictions,
        iff(c_share_union.mental_health_settings is null, '', c_share_union.mental_health_settings) as mental_health_settings,
        iff(c_share_union.residential_services is null, '', c_share_union.residential_services) as residential_services,
        iff(service_types.service_types is null, '', service_types.service_types) as service_types,
         iff(c_share_union.latitude is null, '', c_share_union.latitude) as latitude,
        iff(c_share_union.longitude is null, '', c_share_union.longitude) as longitude,
        -- 6/15 BR update for map_coordinates
        iff(c_share_union.map_coordinates is null, '', c_share_union.map_coordinates) as map_coordinates,
        iff(map_popup_cte.map_popup is null, '', map_popup_cte.map_popup) as map_popup,
        c_share_union.bhe_updated as bhe_updated,
        case 
            when c_share_union.hide_address = 'TRUE' then 'yes' 
            when c_share_union.hide_address = 'FALSE' then 'no'
            else null end as hide_address,
        case 
            when c_share_union.opioid_treatment_provider = 'TRUE' then 'yes'
            when c_share_union.opioid_treatment_provider = 'FALSE' then 'no'
            else null end as opioid_treatment_provider,
        case 
            when c_share_union.residential_child_care_facility = 'TRUE' then 'yes'
            when c_share_union.residential_child_care_facility = 'FALSE' then 'no'
            else null end as residential_child_care_facility,
         case 
            when c_share_union.hospital = 'TRUE' then 'yes'
            when c_share_union.hospital = 'FALSE' then 'no'
            else null end as hospital,      
         case 
            when c_share_union.community_mental_health_center = 'TRUE' then 'yes'
            when c_share_union.community_mental_health_center = 'FALSE' then 'no'
            else null end as community_mental_health_center,  
         case 
            when c_share_union.community_mental_health_clinic = 'TRUE' then 'yes'
            when c_share_union.community_mental_health_clinic = 'FALSE' then 'no'
            else null end as community_mental_health_clinic, 
        -- iff(c_share_union.substance_use_services = '', null, c_share_union.substance_use_services) as substance_use_services,
        
         case 
            when c_share_union.outpatient_su_services = 'TRUE' then 'yes'
            when c_share_union.outpatient_su_services = 'FALSE' then 'no'
            else null end as outpatient_su_services, 
         case 
            when c_share_union.intensive_outpatient_su_services = 'TRUE' then 'yes'
            when c_share_union.intensive_outpatient_su_services = 'FALSE' then 'no'
            else null end as intensive_outpatient_su_services, 
         case 
            when c_share_union.clinic_managed_low_intense_res_svcs = 'TRUE' then 'yes'
            when c_share_union.clinic_managed_low_intense_res_svcs = 'FALSE' then 'no'
            else null end as clinic_managed_low_intense_res_svcs,
         case 
            when c_share_union.clinic_managed_med_intense_res_svcs = 'TRUE' then 'yes'
            when c_share_union.clinic_managed_med_intense_res_svcs = 'FALSE' then 'no'
            else null end as clinic_managed_med_intense_res_svcs,
         case 
            when c_share_union.clinic_managed_high_intense_res_svcs = 'TRUE' then 'yes'
            when c_share_union.clinic_managed_high_intense_res_svcs = 'FALSE' then 'no'
            else null end as clinic_managed_high_intense_res_svcs,
         case 
            when c_share_union.medically_monitored_intense_res_trtmt = 'TRUE' then 'yes'
            when c_share_union.medically_monitored_intense_res_trtmt = 'FALSE' then 'no'
            else null end as medically_monitored_intense_res_trtmt,
         case 
            when c_share_union.clinic_managed_residential_detox = 'TRUE' then 'yes'
            when c_share_union.clinic_managed_residential_detox = 'FALSE' then 'no'
            else null end as clinic_managed_residential_detox,
         case 
            when c_share_union.med_monitored_inpatient_detox = 'TRUE' then 'yes'
            when c_share_union.med_monitored_inpatient_detox = 'FALSE' then 'no'
            else null end as med_monitored_inpatient_detox,

       
        --update checks
        case
            when c_prod.external_id is not null and nvl(c_prod.case_name, '') <> nvl(c_share_union.case_name, '') then 'update_case_name' else null
        end as case_name_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.display_name, '') <> nvl(c_share_union.display_name, '') then 'update_display_name' else null
        end as display_name_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.account_name, '') <> nvl(c_share_union.account_name, '') then 'update_account_name' else null
        end as account_name_action,
        -- 4/10: Added county check
        case
            when c_prod.external_id is not null and nvl(c_prod.county, '') <> nvl(c_share_union.county, '') then 'update_county' else null
        end as county_action,        
        case
            when c_prod.external_id is not null and nvl(c_prod.address_city, '') <> nvl(c_share_union.address_city, '') then 'update_address_city' else null
        end as address_city_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.address_full, '') <> nvl(c_share_union.address_full, '') then 'update_address_full' else null
        end as address_full_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.address_state, '') <> nvl(c_share_union.address_state, '') then 'update_address_state' else null
        end as address_state_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.address_street, '') <> nvl(c_share_union.address_street, '') then 'update_address_street' else null
        end as address_street_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.address_zip, '') <> nvl(c_share_union.address_zip, '') then 'update_address_zip' else null
        end as address_zip_action,
        case
            when c_prod.external_id is not null and nvl(trim(c_prod.clinic_type), '') <> nvl(trim(clinic_type.clinic_type), '') then 'update_clinic_type' else null
        end as clinic_type_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.phone_number::string, '') <> nvl(c_share_union.phone::string, '') then 'update_phone_number' else null
        end as phone_number_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.phone_details, '') <> nvl(c_share_union.phone_details , '') then 'update_phone_details' else null
        end as phone_details_action,
        case
            when c_prod.external_id is not null and nvl(c_prod.phone_display, '') <> nvl(c_share_union.phone_display , '') then 'update_phone_display' else null
        end as phone_display_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.opioid_treatment_provider::string, '') <> nvl(c_share_union.opioid_treatment_provider::string, '') 
                then 'update_opioid_treatment_provider' else null
        end as opioid_treatment_provider_action,
      
        
        --5/28 sprint D: BR include new fields
        case
        -- 6/6 BR updated to wrap in varchar and check with nvl. 
        when c_prod.external_id is not null and nvl(to_varchar(c_prod.original_licensure_date), '') <> nvl(to_varchar(c_share_union.original_licensure_date), '') then 'original_licensure_date' else null
        end as original_licensure_date_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.sud_license_number, '') <> nvl(c_share_union.sud_license_number, '') then 'sud_license_number' else null
        end as sud_license_number_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.cs_license_number, '') <> nvl(c_share_union.cs_license_number, '') then 'cs_license_number' else null
        end as cs_license_number_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.mh_designation, '') <> nvl(c_share_union.mh_designation, '') then 'mh_designation' else null
        end as mh_designation_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.rsso_license_number, '') <> nvl(c_share_union.rsso_license_number, '') then 'rsso_license_number' else null
        end as rsso_license_number_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.offers_telehealth::string, '') <> nvl(c_share_union.offers_telehealth::string, '') then 'offers_telehealth' else null
        -- when c_prod.external_id is not null and try_to_boolean(c_prod.offers_telehealth) <> try_to_boolean(c_share_union.offers_telehealth) then 'offers_telehealth' else null
        end as offers_telehealth_action,
        case
        when c_prod.external_id is not null and nvl(to_varchar(c_prod.fax_number), '') <> nvl(to_varchar(c_share_union.fax_number), '') then 'fax_number' else null
        end as fax_number_action,
        case
        when c_prod.external_id is not null and nvl(to_varchar(c_prod.tdd_tty), '') <> nvl(to_varchar(tdd_tty_calc), '') then 'tdd_tty' else null
        end as tdd_tty_action,
        case
        -- when c_prod.external_id is not null and try_to_boolean(c_prod.hide_address) <> try_to_boolean(c_share_union.hide_address) then 'hide_address' else null
        -- when c_prod.external_id is not null and nvl(c_prod.hide_address, '') <> nvl(c_share_union.hide_address, '') then 'hide_address' else null
         when c_prod.external_id is not null and nvl(c_prod.hide_address::string, '') <> nvl(c_share_union.hide_address::string, '') 
                then 'hide_address' else null --6/6 BR update -- hide_address should have value of 'yes'/'no'
        end as hide_address_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.provider_location_display_label, '') <> nvl(c_share_union.provider_location_display_label, '') then 'provider_location_display_label' else null
        end as provider_location_display_label_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.website, '') <> nvl(c_share_union.website, '') then 'website' else null
        end as website_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.npi, '') <> nvl(c_share_union.npi, '') then 'npi' else null
        end as npi_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.population_served, '') <> nvl(c_share_union.population_served, '') then 'population_served' else null
        end as population_served_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.gender, '') <> nvl(c_share_union.gender, '') then 'gender' else null
        end as gender_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.accessibility, '') <> nvl(c_share_union.accessibility, '') then 'accessibility' else null
        end as accessibility_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.insurance, '') <> nvl(c_share_union.insurance, '') then 'insurance' else null
        end as insurance_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.language_services, '') <> nvl(c_share_union.language_services, '') then 'language_services' else null
        end as language_services_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.monday_hours, '') <> nvl(c_share_union.monday_hours, '') then 'monday_hours' else null
        end as monday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.monday_open, '') <> nvl(c_share_union.monday_open, '') then 'monday_open' else null
        end as monday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.monday_close, '') <> nvl(c_share_union.monday_close, '') then 'monday_close' else null
        end as monday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.tuesday_hours, '') <> nvl(c_share_union.tuesday_hours, '') then 'tuesday_hours' else null
        end as tuesday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.tuesday_open, '') <> nvl(c_share_union.tuesday_open, '') then 'tuesday_open' else null
        end as tuesday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.tuesday_close, '') <> nvl(c_share_union.tuesday_close, '') then 'tuesday_close' else null
        end as tuesday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.wednesday_hours, '') <> nvl(c_share_union.wednesday_hours, '') then 'wednesday_hours' else null
        end as wednesday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.wednesday_open, '') <> nvl(c_share_union.wednesday_open, '') then 'wednesday_open' else null
        end as wednesday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.wednesday_close, '') <> nvl(c_share_union.wednesday_close, '') then 'wednesday_close' else null
        end as wednesday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.thursday_hours, '') <> nvl(c_share_union.thursday_hours, '') then 'thursday_hours' else null
        end as thursday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.thursday_open, '') <> nvl(c_share_union.thursday_open, '') then 'thursday_open' else null
        end as thursday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.thursday_close, '') <> nvl(c_share_union.thursday_close, '') then 'thursday_close' else null
        end as thursday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.friday_hours, '') <> nvl(c_share_union.friday_hours, '') then 'friday_hours' else null
        end as friday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.friday_open, '') <> nvl(c_share_union.friday_open, '') then 'friday_open' else null
        end as friday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.friday_close, '') <> nvl(c_share_union.friday_close, '') then 'friday_close' else null
        end as friday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.saturday_hours, '') <> nvl(c_share_union.saturday_hours, '') then 'saturday_hours' else null
        end as saturday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.saturday_open, '') <> nvl(c_share_union.saturday_open, '') then 'saturday_open' else null
        end as saturday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.saturday_close, '') <> nvl(c_share_union.saturday_close, '') then 'saturday_close' else null
        end as saturday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.sunday_hours, '') <> nvl(c_share_union.sunday_hours, '') then 'sunday_hours' else null
        end as sunday_hours_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.sunday_open, '') <> nvl(c_share_union.sunday_open, '') then 'sunday_open' else null
        end as sunday_open_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.sunday_close, '') <> nvl(c_share_union.sunday_close, '') then 'sunday_close' else null
        end as sunday_close_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.circle_program::string, '') <> nvl(c_share_union.circle_program::string, '') 
                then 'circle_program' else null --6/6 BR update -- circle_program has value of 'No' or blank only
        end as circle_program_action, 
        case
        when c_prod.external_id is not null and nvl(c_prod.mso_affiliation, '') <> nvl(c_share_union.mso_affiliation, '') then 'mso_affiliation' else null
        end as mso_affiliation_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.rae, '') <> nvl(c_share_union.rae, '') then 'rae' else null
        end as rae_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.aso, '') <> nvl(c_share_union.aso, '') then 'aso' else null
        end as aso_action,
        case
        -- when c_prod.external_id is not null and try_to_date(c_prod.provider_directory_form_modified_date) <> try_to_date(c_share_union.provider_directory_form_modified_date) then 'provider_directory_form_modified_date' else null
        when c_prod.external_id is not null and nvl(to_varchar(c_prod.provider_directory_form_modified_date), '') <> nvl(to_varchar(c_share_union.provider_directory_form_modified_date), '') then 'provider_directory_form_modified_date' else null
        end as provider_directory_form_modified_date_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.accepting_new_patients::string, '') <> nvl(c_share_union.accepting_new_patients::string, '') then 'accepting_new_patients' else null
        -- when c_prod.external_id is not null and try_to_boolean(c_prod.accepting_new_patients) <> try_to_boolean(c_share_union.accepting_new_patients) then 'accepting_new_patients' else null
        end as accepting_new_patients_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.telehealth_restrictions::string, '') <> nvl(c_share_union.telehealth_restrictions::string, '') then 'telehealth_restrictions' else null
        -- when c_prod.external_id is not null and try_to_boolean(c_prod.telehealth_restrictions) <> try_to_boolean(c_share_union.telehealth_restrictions) then 'telehealth_restrictions' else null
        end as telehealth_restrictions_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.residential_services, '') <> nvl(c_share_union.residential_services, '') then 'residential_services' else null
        end as residential_services_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.mental_health_settings, '') <> nvl(c_share_union.mental_health_settings, '') then 'mental_health_settings' else null
        end as mental_health_settings_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.service_types, '') <> nvl(service_types.service_types, '') then 'service_types' else null
        end as service_types_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.latitude, '') <> nvl(c_share_union.latitude, '') then 'latitude' else null
        end as latitude_action, 
        case
        when c_prod.external_id is not null and nvl(c_prod.longitude, '') <> nvl(c_share_union.longitude, '') then 'longitude' else null
        end as longitude_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.map_coordinates, '') <> nvl(c_share_union.map_coordinates, '') then 'map_coordinates' else null
        end as map_coordinates_action,
        case
        when c_prod.external_id is not null and nvl(c_prod.map_popup, '') <> nvl(map_popup_cte.map_popup, '') then 'map_popup' else null
        end as map_popup_action,
		case 
		when c_prod.external_id is not null and nvl(c_prod.owner_id, '') <> nvl(locs.location_id, '') then 'owner' else null
		end as owner_action,
        case 
		when c_prod.external_id is not null and nvl(c_prod.bhe_updated::string, '') <> nvl(c_share_union.bhe_updated::string, '') then 'bhe_updated' else null
		end as bhe_updated_action,
        case 
        when c_prod.external_id is not null and (
                 case_name_action is not null 
                  or display_name_action is not null
                  or account_name_action is not null 
                  or address_city_action is not null 
                  or address_full_action is not null or address_state_action is not null 
                  or address_street_action is not null
                  or address_zip_action is not null or clinic_type_action is not null 
                  or phone_number_action is not null 
                  or phone_details_action is not null 
                  or phone_display_action is not null
                  or opioid_treatment_provider_action is not null 
                  -- or substance_use_services_action is not null
                  or county_action is not null
                  -- BR build work 
                  or original_licensure_date_action is not null
                  or sud_license_number_action  is not null
                  or cs_license_number_action  is not null
                  or mh_designation_action  is not null
                  or rsso_license_number_action  is not null
                  or offers_telehealth_action  is not null
                  or fax_number_action  is not null
                  or tdd_tty_action  is not null
                  or hide_address_action  is not null
                  or provider_location_display_label_action  is not null
                  or website_action  is not null
                  or population_served_action  is not null
                  or gender_action  is not null
                  or accessibility_action  is not null
                  or npi_action  is not null
                  or  insurance_action  is not null
                  or language_services_action  is not null
                  or monday_hours_action  is not null
                  or monday_open_action  is not null
                  or monday_close_action  is not null
                  or tuesday_hours_action  is not null
                  or tuesday_open_action  is not null
                  or tuesday_close_action  is not null
                  or wednesday_hours_action  is not null
                  or wednesday_open_action  is not null
                  or wednesday_close_action  is not null
                  or thursday_hours_action  is not null
                  or thursday_open_action  is not null
                  or thursday_close_action  is not null
                  or friday_hours_action  is not null
                  or friday_open_action  is not null
                  or friday_close_action  is not null
                  or saturday_hours_action  is not null
                  or saturday_open_action  is not null
                  or saturday_close_action  is not null
                  or sunday_hours_action  is not null
                  or sunday_open_action  is not null
                  or sunday_close_action  is not null
                  or  circle_program_action  is not null
                  or mso_affiliation_action  is not null
                  or rae_action  is not null
                  or aso_action  is not null
                  or provider_directory_form_modified_date_action  is not null
                  or accepting_new_patients_action  is not null
                  or telehealth_restrictions_action  is not null
                  or residential_services_action  is not null
                  or mental_health_settings_action  is not null
                  or service_types_action  is not null
                  or 
                  latitude_action  is not null -- KC 6/1: Corrected to correct name
                  or 
                  longitude_action  is not null -- KC 6/1: Corrected to correct name
                  or 
                  map_coordinates_action  is not null -- BR 6/15
                  -- or 
                  -- map_popup_action is not null
                  or owner_action is not null
                  or bhe_updated_action is not null
                  ) then 'update' 
        when c_prod.external_id is null and c_share_union.ladders_external_id is not null then 'create'
        else null end as action,
        current_timestamp() as import_date --,
        -- 12/1 for tile_header | 12/4 BR: commented out tile_header related
        -- concat(tile_header.text_before_display_name,tile_header.display_name, tile_header.text_before_phone_display, tile_header.phone_display, tile_header.text_before_address_full, tile_header.address_full, tile_header.text_before_mental_health_settings, RTRIM(tile_header.mental_health_settings,', '),tile_header.text_before_insurance,  RTRIM(tile_header.insurance,', '), tile_header.text_before_referral_type, RTRIM(tile_header.referral_type,', '), tile_header.text_before_exclusions, RTRIM(tile_header.exclusions,', '), tile_header.text_view_more_info_link1, tile_header.project_space, tile_header.text_view_more_info_link2) as tile_header
from  c_share_union left join c_prod on c_prod.external_id = c_share_union.ladders_external_id 
              left join clinic_type on c_share_union.ladders_external_id = clinic_type.ladders_external_id
              --5/28 sprint D: BR include new fields
              left join service_types on c_share_union.ladders_external_id = service_types.ladders_external_id
              left join map_popup_cte on c_share_union.ladders_external_id = map_popup_cte.ladders_external_id
              -- 12/1 for tile_header | 12/4 BR: commented out tile_header related
              -- left join tile_header on c_share_union.ladders_external_id = tile_header.ladders_external_id
              left join p_prod on p_prod.external_id = c_share_union.provider_external_id
              left join locs on locs.site_code = c_share_union.ladders_external_id
where action is not null 
order by action,ladders_external_id, c_prod.date_opened, c_share_union.case_name
 )
 
 select 
 	PROD_EXTERNAL_ID,
	PROD_CASE_ID,
	PROD_DATE_OPENED,
	LADDERS_EXTERNAL_ID,
	OWNER_ID,
	CASE_TYPE,
	PARENT_RELATIONSHIP,
	PARENT_CASE_TYPE,
	PARENT_CASE_ID,
	PROVIDER_EXTERNAL_ID,
	CASE_NAME,
	DISPLAY_NAME,
    ACCOUNT_NAME,
	COUNTY,
	ADDRESS_CITY,
	ADDRESS_FULL,
	ADDRESS_STATE,
	ADDRESS_STREET,
	ADDRESS_ZIP,
	CLINIC_TYPE,
	PHONE,
	PHONE_DETAILS,
	PHONE_DISPLAY,
	ORIGINAL_LICENSURE_DATE,
	SUD_LICENSE_NUMBER,
	CS_LICENSE_NUMBER,
	MH_DESIGNATION,
	RSSO_LICENSE_NUMBER,
	OFFERS_TELEHEALTH,
	FAX_NUMBER,
	TDD_TTY,
	PROVIDER_LOCATION_DISPLAY_LABEL,
	WEBSITE,
	POPULATION_SERVED,
	GENDER,
	ACCESSIBILITY,
	INSURANCE,
	LANGUAGE_SERVICES,
	NPI,
	MONDAY_HOURS,
	MONDAY_OPEN,
	MONDAY_CLOSE,
	TUESDAY_HOURS,
	TUESDAY_OPEN,
	TUESDAY_CLOSE,
	WEDNESDAY_HOURS,
	WEDNESDAY_OPEN,
	WEDNESDAY_CLOSE,
	THURSDAY_HOURS,
	THURSDAY_OPEN,
	THURSDAY_CLOSE,
	FRIDAY_HOURS,
	FRIDAY_OPEN,
	FRIDAY_CLOSE,
	SATURDAY_HOURS,
	SATURDAY_OPEN,
	SATURDAY_CLOSE,
	SUNDAY_HOURS,
	SUNDAY_OPEN,
	SUNDAY_CLOSE,
	CIRCLE_PROGRAM,
	MSO_AFFILIATION,
	RAE,
	ASO,
	PROVIDER_DIRECTORY_FORM_MODIFIED_DATE,
	ACCEPTING_NEW_PATIENTS,
	TELEHEALTH_RESTRICTIONS,
	MENTAL_HEALTH_SETTINGS,
	RESIDENTIAL_SERVICES,
	SERVICE_TYPES,
	LATITUDE,
	LONGITUDE,
	MAP_COORDINATES,
	MAP_POPUP,
	HIDE_ADDRESS,
	OPIOID_TREATMENT_PROVIDER,
	RESIDENTIAL_CHILD_CARE_FACILITY,
	HOSPITAL,
	COMMUNITY_MENTAL_HEALTH_CENTER,
	COMMUNITY_MENTAL_HEALTH_CLINIC,
	OUTPATIENT_SU_SERVICES,
	INTENSIVE_OUTPATIENT_SU_SERVICES,
	CLINIC_MANAGED_LOW_INTENSE_RES_SVCS,
	CLINIC_MANAGED_MED_INTENSE_RES_SVCS,
	CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS,
	MEDICALLY_MONITORED_INTENSE_RES_TRTMT,
	CLINIC_MANAGED_RESIDENTIAL_DETOX,
	MED_MONITORED_INPATIENT_DETOX,
    BHE_UPDATED,
	CASE_NAME_ACTION,
	DISPLAY_NAME_ACTION,
    ACCOUNT_NAME_ACTION,
	COUNTY_ACTION,
	ADDRESS_CITY_ACTION,
	ADDRESS_FULL_ACTION,
	ADDRESS_STATE_ACTION,
	ADDRESS_STREET_ACTION,
	ADDRESS_ZIP_ACTION,
	CLINIC_TYPE_ACTION,
	PHONE_NUMBER_ACTION,
	PHONE_DETAILS_ACTION,
	PHONE_DISPLAY_ACTION,
	OPIOID_TREATMENT_PROVIDER_ACTION,
	ORIGINAL_LICENSURE_DATE_ACTION,
	SUD_LICENSE_NUMBER_ACTION,
	CS_LICENSE_NUMBER_ACTION,
	MH_DESIGNATION_ACTION,
	RSSO_LICENSE_NUMBER_ACTION,
	OFFERS_TELEHEALTH_ACTION,
	FAX_NUMBER_ACTION,
	TDD_TTY_ACTION,
	HIDE_ADDRESS_ACTION,
	PROVIDER_LOCATION_DISPLAY_LABEL_ACTION,
	WEBSITE_ACTION,
	NPI_ACTION,
	POPULATION_SERVED_ACTION,
	GENDER_ACTION,
	ACCESSIBILITY_ACTION,
	INSURANCE_ACTION,
	LANGUAGE_SERVICES_ACTION,
	MONDAY_HOURS_ACTION,
	MONDAY_OPEN_ACTION,
	MONDAY_CLOSE_ACTION,
	TUESDAY_HOURS_ACTION,
	TUESDAY_OPEN_ACTION,
	TUESDAY_CLOSE_ACTION,
	WEDNESDAY_HOURS_ACTION,
	WEDNESDAY_OPEN_ACTION,
	WEDNESDAY_CLOSE_ACTION,
	THURSDAY_HOURS_ACTION,
	THURSDAY_OPEN_ACTION,
	THURSDAY_CLOSE_ACTION,
	FRIDAY_HOURS_ACTION,
	FRIDAY_OPEN_ACTION,
	FRIDAY_CLOSE_ACTION,
	SATURDAY_HOURS_ACTION,
	SATURDAY_OPEN_ACTION,
	SATURDAY_CLOSE_ACTION,
	SUNDAY_HOURS_ACTION,
	SUNDAY_OPEN_ACTION,
	SUNDAY_CLOSE_ACTION,
	CIRCLE_PROGRAM_ACTION,
	MSO_AFFILIATION_ACTION,
	RAE_ACTION,
	ASO_ACTION,
	PROVIDER_DIRECTORY_FORM_MODIFIED_DATE_ACTION,
	ACCEPTING_NEW_PATIENTS_ACTION,
	TELEHEALTH_RESTRICTIONS_ACTION,
	RESIDENTIAL_SERVICES_ACTION,
	MENTAL_HEALTH_SETTINGS_ACTION,
	SERVICE_TYPES_ACTION,
	LATITUDE_ACTION,
	LONGITUDE_ACTION,
	MAP_COORDINATES_ACTION,
	MAP_POPUP_ACTION,
    OWNER_ACTION,
    BHE_UPDATED_ACTION,
	ACTION,
	IMPORT_DATE
from final