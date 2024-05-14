{% macro create_fn_get_clinic_type_of_care_display() %}

CREATE OR REPLACE FUNCTION 
    {% if target.name=='dev' %}
      DM_LADDERS_DEV.DM
    {% elif target.name=='qa' %}
      DM_LADDERS_QA.DM
    {% elif target.name=='prod' %}
      DM_LADDERS_PROD.DM
    {% elif target.name=='test' %}
      DM_LADDERS_TEST.DM
    {% else %}
      invalid
    {% endif %}

-- .DM.FN_GET_CLINIC_TYPE_OF_CARE_DISPLAY(residential_services VARCHAR(), mental_health_settings varchar())
.FN_GET_CLINIC_TYPE_OF_CARE_DISPLAY("residential_services" VARCHAR(16777216), "mental_health_settings" VARCHAR())
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS '{{ fn_get_clinic_type_of_care_display() }}';

{% endmacro %}