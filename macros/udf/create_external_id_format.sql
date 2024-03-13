{% macro create_external_id_format() %}

CREATE OR REPLACE FUNCTION DM.EXTERNAL_ID_FORMAT("S" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    HANDLER = 'cleansing'
AS '{{ external_id_format() }}';
  
{% endmacro %}
