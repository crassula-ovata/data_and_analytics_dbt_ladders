with 
dm_table_data_clinic_open as (
    select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
    where closed = false 
)

select * 
from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} 
where
    action = 'create'
    and case_name in (select case_name from dm_table_data_clinic_open)
