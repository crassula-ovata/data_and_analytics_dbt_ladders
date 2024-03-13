{% macro external_id_format() %}

def cleansing(x):
    import re
    import string
     
    # add _ after cap letters
    x =  re.sub(r''([A-Z])'', r''\\1_'', x)
    
    # lower case all letters
    x = x.lower()

     
    return x
  
{% endmacro %}
