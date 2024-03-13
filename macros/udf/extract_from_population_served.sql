{% macro extract_from_population_served() %}

import re
from collections import OrderedDict

gender_list = [
''men'',
''non_binary_genderqueer_gender_fluid'',
''option_not_listed'',
''no_gender_restrictions'',
''transgender_man_trans_masculine'',
''transgender_woman_trans_feminine'',
''two_spirit'',
''women'']

def extract_from_population_served(population_served_original, gender):
    population_served_original = population_served_original.replace(''Youth'', ''Minors/Adolescents'')
    population_served_original = population_served_original.replace(''+'', '''')
    parts = population_served_original.split(''; '') 
    parts_unique = list(OrderedDict.fromkeys(parts))
    
    cleaned_parts = [re.sub(''[^a-z0-9]+'', ''_'', value.lower()) for value in parts_unique]
    
    if gender:
        gender_values = [item for item in cleaned_parts if item in gender_list]
        result_string = '' ''.join(gender_values)
        return result_string
    else:
        non_gender_values = [item for item in cleaned_parts if item not in gender_list]
        result_string = '' ''.join(non_gender_values)
        return result_string
  
{% endmacro %}
