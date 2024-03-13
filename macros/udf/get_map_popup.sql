{% macro get_map_popup() %}

   select concat( coalesce(display_name, ''No Information Available''), ''%'', 
        ''* **Phone:** '', coalesce(phone_display, ''No Information Available''), ''%'',
        ''* **Address:** '', coalesce(address_full, ''No Information Available''), ''%'',
        ''* **Insurance:** '', 
        regexp_replace(replace(replace(replace(replace(coalesce(insurance, ''No Information Available''), ''medicaid'', ''Medicaid / Health First Colorado,''), ''private_insurance'', ''Private Insurance,''), ''self_pay'', ''Self-Pay,''),
    ''sliding_fee_scale'', ''Sliding Fee Scale,''),'',$'', '''')
        , ''%'',
        ''* **Referral Types:** '', 
        regexp_replace(
        replace(replace(replace(replace(replace(coalesce(referral_type, ''No Information Available''), ''court_mandated_referrals_only'', ''Court Mandated Referrals Only,''), ''insurance_authorization'', ''Insurance Pre-Authorization,''), ''physical_assessments'', ''Physical Assessments,''),''psychiatric_assessments'', ''Psychiatric Assessments,''), ''walk_in'', ''Walk-In,'')
        , '',$'', '''')
        ) 
    as map_popup
      
{% endmacro %}

