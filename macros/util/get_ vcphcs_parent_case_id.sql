{% macro get_vcphcs_parent_case_id(param_type) %}

    {% if target.name=='dev' or target.name=='sfstaging-dev' %}
        {% set child_value1 = '\'19675d8a-9306-4d3d-b1c0-7e8892726867\', \'d182b189-2fab-49fa-8c42-7ced2237c9de\', \'4b3506a7-82da-4e3a-b64a-c1d969ebe0ae\'' %}
        {% set parent_value1 = 'parentcaseid-dev1' %}
        {% set child_value2 = '\'31ad557d-a3b9-4d5e-987c-bf4ed68c0528\', \'fa0ca9ae-45e7-4d8d-904e-fce9d9585f77\', \'5c39f911-669c-4eca-8512-fa6afb2a15ad\'' %}
        {% set parent_value2 = 'parentcaseid-dev2' %}
    {% elif target.name=='qa' or target.name=='sfstaging-qa' %}
        {% set child_value1 = '\'111\', \'222\', \'333\'' %}
        {% set parent_value1 = 'parentcaseid-qa1' %}
        {% set child_value2 = '\'111\', \'222\', \'333\'' %}
        {% set parent_value2 = 'parentcaseid-qa2' %}
    {% elif target.name=='prod' or target.name=='sfstaging-prod' %}
        {% set child_value1 = '\'162bb681-a621-4a8f-a534-72feca54cdbe\', \'ce3cbc37-fd1c-4e44-b970-178260c1b63c\', \'0016100001H9ZZDAAB-0016100001JGBJKAAJ\'' %}
        {% set parent_value1 = 'parentcaseid-prod1' %}
        {% set child_value2 = '\'f07a5bc2-f677-4490-bb1b-80cdc78e5334\', \'bd32d9f3-8c8a-4749-88c0-ac8ac56b7e4d\', \'bbf8b5ed-70f8-44b8-86d6-27a9783062e2\'' %}
        {% set parent_value2 = 'parentcaseid-prod2' %}
    {% elif target.name=='test' or target.name=='sfstaging-test' %}
        {% set child_value1 = '\'07cae0c3dc7046cf861d8bf40d2e3ddd\', \'bf384796ed654d2eb252cd0b506dc9b9\', \'ccb5e1a3ec274cba9d5fa7e30a0c3522\'' %}
        {% set parent_value1 = 'parentcaseid-test1' %}
        {% set child_value2 = '\'f220881f855d44eda59081f49527ca2e\', \'6d65421f053e4209881e0b5e2a52dd16\', \'a182b33ef7a34995946ef5ffc2d76aa0\'' %}
        {% set parent_value2 = 'parentcaseid-test2' %}
    {% elif target.name=='test-perf' or target.name=='sfstaging-test-perf' %}
        {% set child_value1 = '\'2489b4bbf83d4bfc8350bd5ddce4e9ec\', \'e9e5eae550904e188450358353c86be9\', \'2dd46bdf76534eaf8ab265ccae43d20b\'' %}
        {% set parent_value1 = 'parentcaseid-perf1' %}
        {% set child_value2 = '\'c65f34422d1e4b96acd29a11b23f6855\', \'799e0cf35a3847b4b4f765ce35b055bd\', \'bd6a5830c0f84f3b8c8c9a01055a6379\'' %}
        {% set parent_value2 = 'parentcaseid-perf2' %}
    {% elif target.name=='test-staging' or target.name=='sfstaging-test-staging' %}
        {% set child_value1 = '\'111\', \'222\', \'333\'' %}
        {% set parent_value1 = 'parentcaseid-test-staging1' %}
        {% set child_value2 = '\'111\', \'222\', \'333\'' %}
        {% set parent_value2 = 'parentcaseid-test-staging2' %}
    {% else %}
        {% set child_value1 = '' %}
        {% set parent_value1 = '' %}
        {% set child_value2 = '' %}
        {% set parent_value2 = '' %}
    {% endif %}

    {% if param_type=='child_value1' %}
        {{ return(child_value1) }}
    {% elif param_type=='child_value2' %}
        {{ return(child_value2) }}
    {% elif param_type=='parent_value1' %}
        {{ return(parent_value1) }}
    {% elif param_type=='parent_value2' %}
        {{ return(parent_value2) }}
    {% endif %}   

{% endmacro %}