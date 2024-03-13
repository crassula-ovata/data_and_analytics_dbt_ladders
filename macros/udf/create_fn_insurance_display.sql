{% macro create_fn_insurance_display() %}

CREATE OR REPLACE FUNCTION DM.FN_INSURANCE_DISPLAY("INSURANCE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
AS '{{ fn_insurance_display() }}';
  
{% endmacro %}
