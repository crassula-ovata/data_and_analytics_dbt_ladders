{% macro fn_validate_lat() %}

case 
    when latitude between -90 and 90 and not latitude = 0 
        then latitude else null 
end
  
{% endmacro %}