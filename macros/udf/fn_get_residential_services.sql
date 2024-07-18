{% macro fn_get_residential_services() %}

TRIM(CONCAT('' '', 
        IFF(DUI_DWI, ''dui_dwi '', ''''),
        IFF(EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS, ''education_and_treatment_services_for_persons_in_criminal_justice_system '', ''''),
        IFF(CLINIC_MANAGED_LOW_INTENSE_RES_SVCS, ''clinically_managed_low_intensity_residential_services '', ''''),
        IFF(CLINIC_MANAGED_MED_INTENSE_RES_SVCS, ''clinically_managed_medium_intensity_residential_services '', ''''),
        IFF(CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS, ''clinically_managed_high_intensity_residential_services '', ''''),
        IFF(CLINIC_MANAGED_RESIDENTIAL_DETOX, ''clinically_managed_residential_detoxification '', ''''),
        IFF(OPIOID_TREATMENT_PROGRAMS, ''opioid_treatment_programs '', ''''),
        IFF(GENDER_RESPONSIVE_TTMT_FOR_WOMEN, ''gender_responsive_treatment_for_women '', ''''),
        IFF(DAY_TREATMENT_PARTIAL_HOSPITALIZATION, ''day_treatment_partial_hospitalization '', ''''),
        IFF(MED_MONITORED_INPATIENT_DETOX, ''medically_monitored_inpatient_detoxification '', ''''),
        IFF(MEDICALLY_MONITORED_INTENSE_RES_TRTMT, ''medically_monitored_intensive_residential_treatment '', '''')
    )) 

{% endmacro %}