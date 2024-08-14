with 
payloads as (
    select * from {{ ref('VW_SITE_CODE_UPDATE') }}
)
,numbered_payloads as ( --add row numbers and groupings of 100 at a time
    select row_number() over(order by LOC_ID asc) rownum, ceil(rownum/100) grouping, payload payloads from payloads
)
,final as (
select grouping, '{"objects": [' || listagg(payloads::string, ',') || ']}' payload --concatenate payloads into arrays of up to 100
from numbered_payloads group by 1 having payload <> '{"objects": []}' order by 1
)
select 
	GROUPING,
	PAYLOAD
from final