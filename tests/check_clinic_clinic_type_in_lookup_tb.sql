

with 
dm_table_data_fix_clinic_type as (
    select * from  {{ source('dm_table_data', 'FIXTURE_CLINIC_TYPE') }}
),
cte_clinic_type as 
(
    select
        prod_case_id,
        clinic_type,
        flat_values.value::string as flat_value
    from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} ,
        lateral flatten(input => split(clinic_type, ' ')) as flat_values
    where
        clinic_type_action = 'clinic_type'
) 
select * 
from cte_clinic_type
where 
 NOT EXISTS (SELECT 1 FROM dm_table_data_fix_clinic_type fx where fx.value= cte_clinic_type.flat_value )