{% macro fn_validate_long() %}

case 
    when longitude between -180 and 180 and not longitude = 0 
        then longitude else null 
    end as longitude
  
{% endmacro %}