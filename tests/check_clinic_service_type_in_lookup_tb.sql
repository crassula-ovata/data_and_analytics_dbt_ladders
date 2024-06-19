
with 
dm_table_data_fix_service_types as (
    select * from  {{ source('dm_table_data', 'FIXTURE_SERVICE_TYPES') }}
),
cte_service_types as 
(
    select
        flat_values.value::string as flat_value
    from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} ,
        lateral flatten(input => split(service_types, ' ')) as flat_values
    where
        service_types_action = 'service_types'
) 
select * 
from cte_service_types
where 
 NOT EXISTS (SELECT 1 FROM dm_table_data_fix_service_types fx where fx.value= cte_service_types.flat_value )