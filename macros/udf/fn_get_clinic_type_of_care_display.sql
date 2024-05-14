{% macro fn_get_clinic_type_of_care_display() %}
    case 
        when nvl(residential_services, '')  != '' and nvl(mental_health_settings, '')  != '' then 'Both Mental Health & Substance Use'
        when nvl(residential_services, '')  != '' and nvl(mental_health_settings, '')  = '' then 'Substance Use'
        when nvl(residential_services, '')  = '' and nvl(mental_health_settings, '')  != '' then 'Mental Health'
        when nvl(residential_services, '')  = '' and nvl(mental_health_settings, '')  = '' then 'No information available'
    end
{% endmacro %}