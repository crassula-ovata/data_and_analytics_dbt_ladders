{% macro fn_insurance_display() %}

  CASE 
    WHEN "INSURANCE" = '''' OR "INSURANCE" IS NULL THEN NULL
    ELSE 
      rtrim(replace(replace(replace(replace(INSURANCE, 
                ''medicaid'', ''Medicaid / Health First Colorado,''),
                ''private_insurance'', ''Private Insurance,''), 
                ''self_pay'', ''Self-Pay,''), 
                ''sliding_fee_scale'', ''Sliding Fee Scale,''), '','')
  END

{% endmacro %}

