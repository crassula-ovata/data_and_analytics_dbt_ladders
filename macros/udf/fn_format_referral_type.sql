{% macro fn_format_referral_type() %}   

format_map = {
    "court_mandated_referrals_only": "Court Mandated Referrals Only,",
    "insurance_authorization": "Insurance Pre-Authorization,",
    "physical_assessments": "Physical Assessments,",
    "psychiatric_assessments": "Psychiatric Assessments,",
    "walk_in": "Walk-In,"
}

def fn_format_referral_type(REFERRAL_TYPE):
    if not REFERRAL_TYPE:
        return REFERRAL_TYPE 
    else:
        for default_value, formated_value in format_map.items():
            REFERRAL_TYPE = REFERRAL_TYPE.replace(default_value, formated_value)
        REFERRAL_TYPE = REFERRAL_TYPE.rstrip(",")

    return REFERRAL_TYPE

{% endmacro %}