{% macro fn_validate_lat() %}

case 
    when 
        latitude is not null and 
        TRY_TO_NUMBER(latitude, 6) is not null and
        TRY_TO_NUMBER(latitude, 6) between -90 and 90 and 
        TRY_TO_NUMBER(latitude, 6) != 0 
    then latitude 
    else null
end
  
{% endmacro %}