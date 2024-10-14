{% macro create_fn_validate_long() %}

CREATE OR REPLACE FUNCTION DM.FN_VALIDATE_LONG ("LONGITUDE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
AS '{{ fn_validate_long() }}';
{% endmacro %}