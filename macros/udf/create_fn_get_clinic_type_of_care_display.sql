{% macro create_fn_get_clinic_type_of_care_display() %}

CREATE OR REPLACE FUNCTION DM_LADDERS_TEST.DM.FN_GET_CLINIC_TYPE_OF_CARE_DISPLAY(residential_services VARCHAR(), mental_health_settings varchar())
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS 
  $$
    case 
        when nvl(residential_services, '')  != '' and nvl(mental_health_settings, '')  != '' then 'Both Mental Health & Substance Use'
        when nvl(residential_services, '')  != '' and nvl(mental_health_settings, '')  = '' then 'Substance Use'
        when nvl(residential_services, '')  = '' and nvl(mental_health_settings, '')  != '' then 'Mental Health'
        when nvl(residential_services, '')  = '' and nvl(mental_health_settings, '')  = '' then 'No information available'
    end
  $$
;

{% endmacro %}