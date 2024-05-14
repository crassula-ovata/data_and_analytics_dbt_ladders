{% macro create_extract_from_population_served() %}

CREATE OR REPLACE FUNCTION
    {% if target.name=='dev' %}
      DM_LADDERS_DEV
    {% elif target.name=='qa' %}
      DM_LADDERS_QA
    {% elif target.name=='prod' %}
      DM_LADDERS_PROD
    {% elif target.name=='test' %}
      DM_LADDERS_TEST
    {% else %}
      invalid
    {% endif %}

 DM.EXTRACT_FROM_POPULATION_SERVED("POPULATION_SERVED_ORIGINAL" VARCHAR(16777216), "GENDER" BOOLEAN)
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    HANDLER = 'extract_from_population_served'
AS '{{ extract_from_population_served() }}';
  
{% endmacro %}
