{% macro fn_get_mental_health_settings() %}

TRIM(CONCAT('' '', 
        IFF(hospital, ''hospital '', ''''),
        IFF(emergency, ''emergency '', ''''),
        IFF(treatment_evaluation_72_hour, ''72_hour_treatment_and_evaluation '', ''''),
        IFF(residential_long_term_treatment, ''residential_long_term_treatment '', ''''),
        IFF(residential_short_term_treatment, ''residential_short_term_treatment '', ''''),
        IFF(residential_child_care_facility, ''residential_child_care_facility '', ''''),
        IFF(crisis_stabilization_unit, ''crisis_stabilization_unit '', ''''),
        IFF(acute_treatment_unit, ''acute_treatment_unit '', '''')
    )) 

{% endmacro %}