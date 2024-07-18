{% macro create_fn_get_substance_use_services() %}

CREATE OR REPLACE FUNCTION DM.fn_get_substance_use_services(
    "OUTPATIENT" BOOLEAN,
    "INTENSIVE_OUTPATIENT_SU_SERVICES" BOOLEAN,
    "GENERAL_TREATMENT" BOOLEAN,
    "DUI_DWI" BOOLEAN,
    "EDU_TTMT_SVCS_FOR_PERSONS_IN_CJS" BOOLEAN,
    "CLINIC_MANAGED_LOW_INTENSE_RES_SVCS"  BOOLEAN,
    "CLINIC_MANAGED_MED_INTENSE_RES_SVCS"  BOOLEAN,
    "CLINIC_MANAGED_HIGH_INTENSE_RES_SVCS"  BOOLEAN,
    "CLINIC_MANAGED_RESIDENTIAL_DETOX" BOOLEAN,
    "OPIOID_TREATMENT_PROGRAMS" BOOLEAN,
    "GENDER_RESPONSIVE_TTMT_FOR_WOMEN" BOOLEAN,
    "DAY_TREATMENT_PARTIAL_HOSPITALIZATION" BOOLEAN,
    "MED_MONITORED_INPATIENT_DETOX" BOOLEAN,
    "MEDICALLY_MONITORED_INTENSE_RES_TRTMT"  BOOLEAN
    )
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS '{{ fn_get_substance_use_services() }}';
  
{% endmacro %}