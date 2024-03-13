{% macro create_fn_mental_health_settings_display() %}

CREATE OR REPLACE FUNCTION DM.FN_MENTAL_HEALTH_SETTINGS_DISPLAY("MENTAL_HEALTH_SETTINGS" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
AS '{{ fn_mental_health_settings_display() }}';
  
{% endmacro %}
