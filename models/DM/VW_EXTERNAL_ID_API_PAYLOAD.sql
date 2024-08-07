with 
payloads as (
    select * from {{ ref('VW_EXTERNAL_ID_UPDATE') }}
)
,numbered_payloads as ( --add row numbers and groupings of 100 at a time
  select row_number() over(order by CASE_ID asc) rownum, ceil(rownum/100) grouping, payload from payloads
)
,final as (
select grouping, '[' || listagg(payload::string, ',') || ']' payload --concatenate payloads into arrays of up to 100
from numbered_payloads group by 1 order by 1
)
select 
	GROUPING,
	PAYLOAD
from final where nullif(payload, '[]') is not null