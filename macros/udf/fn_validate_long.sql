{% macro fn_validate_long() %}

case 
    when longitude is not null and 
         longitude != '' and 
         TRY_TO_NUMBER(longitude, 6) between -180 and 180 and 
         TRY_TO_NUMBER(longitude, 6) != 0 
    then longitude
    else null 
end
  
{% endmacro %}