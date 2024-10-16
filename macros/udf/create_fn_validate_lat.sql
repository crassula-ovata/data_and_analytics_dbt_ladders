{% macro create_fn_validate_lat() %}

CREATE OR REPLACE FUNCTION DM.FN_VALIDATE_LAT("LATITUDE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
AS '{{ fn_validate_lat() }}';

{% endmacro %}