{% fn_format_referral_type %} macro  

    regexp_replace(
            replace(replace(replace(replace(replace(coalesce(REFERRAL_TYPE, ''N/A''), ''court_mandated_referrals_only'', ''Court Mandated Referrals Only,''), ''insurance_authorization'', ''Insurance Pre-Authorization,''), ''physical_assessments'', ''Physical Assessments,''),''psychiatric_assessments'', ''Psychiatric Assessments,''), ''walk_in'', ''Walk-In,'')
            , '',$'', '''')

{% endmacro %}