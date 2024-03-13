{% macro fn_mental_health_settings_display() %}

  CASE 
    WHEN "MENTAL_HEALTH_SETTINGS" = '''' OR "MENTAL_HEALTH_SETTINGS" IS NULL THEN NULL
    ELSE 
      rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("MENTAL_HEALTH_SETTINGS", 
                ''hospital'', ''Hospital,''),
                ''emergency'', ''Emergency,''), 
                ''outpatient'', ''Outpatient,''), 
                ''intensive_outpatient'', ''Intensive Outpatient,''), 
                ''72_hour_treatment_and_evaluation'', ''72-Hour Treatment & Evaluation,''), 
                ''residential_long_term_treatment'', ''Residential Long Term Treatment,''), 
                ''residential_short_term_treatment'', ''Residential Short Term Treatment,''), 
                ''day_treatment'', ''Day Treatment,''), 
                ''psychiatric_residential'', ''Psychiatric Residential,''), 
                ''community_mental_health_center'', ''Community Mental Health Center,''), 
                ''community_mental_health_clinic'', ''Community Mental Health Clinic,''),
                ''residential_child_care_facility'', ''Residential Child Care Facility,''),
                ''crisis_stabilization_unit'', ''Crisis Stabilization Unit,''),
                ''acute_treatment_unit'', ''Acute Treatment Unit,''), '','' )
  END

{% endmacro %}
