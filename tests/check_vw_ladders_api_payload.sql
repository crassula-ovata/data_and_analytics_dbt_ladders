select payloadObj.value:case_type::string case_type_value, 
       payloadObj.value:create::string create_value,
       payloadObj.value:clinic_case_name_display::string clinic_case_name_display_value
from {{ ref('VW_LADDERS_API_PAYLOAD')}},
      lateral flatten(input => parse_json(payload)) payloadObj
where create_value=false 
      and case_type_value='capacity'
      and clinic_case_name_display_value=''   