
with 
dm_table_data_fix_mental_health_settings as (
    select * from  {{ source('dm_table_data', 'FIXTURE_MENTAL_HEALTH_SETTINGS') }}
),
cte_mental_health_settings as 
(
    select
        prod_case_id,
        mental_health_settings,
        flat_values.value::string as flat_value
    from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} ,
        lateral flatten(input => split(clinic_type, ' ')) as flat_values
    where
        mental_health_settings_action = 'mental_health_settings'
) 
select * 
from cte_mental_health_settings
where 
 NOT EXISTS (SELECT 1 FROM dm_table_data_fix_mental_health_settings fx where fx.value= cte_mental_health_settings.flat_value )