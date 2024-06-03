with 
dm_table_data_clinic_open as (
    select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
    where closed = false 
)

select * 
from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} 
where
    action = 'create'
    and ladders_external_id in (select external_id from dm_table_data_clinic_open)