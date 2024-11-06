{% macro create_fn_format_referral_type() %}

CREATE OR REPLACE FUNCTION DM.FN_FORMAT_REFERRAL_TYPE("REFERRAL_TYPE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    HANDLER = 'fn_format_referral_type'
AS '{{ fn_format_referral_type() }}';
  
{% endmacro %}