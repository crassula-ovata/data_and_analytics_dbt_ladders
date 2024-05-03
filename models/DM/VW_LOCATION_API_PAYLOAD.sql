with locs as (
    select * from  {{ source('dm_table_data', 'LOCATION') }}
),
case_provider as (
    select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
),
case_clinic as (
    select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
),
hades_table_data_ladders_active_licenses as (
      select * from  {{ source('hades_table_data', 'VWS_LADDERS_ACTIVE_LICENSES') }}
), 
ld as (
    select * 
    ,dm.external_id_format (parent_account_id) p_id
    ,dm.external_id_format (account_id) a_id
    from hades_table_data_ladders_active_licenses
),
-- ********** REMOVE SECTION WHEN NO LONGER NEEDED **********
prov as (
    select distinct
    p.external_id LOC_ID, 
    parse_json(
        '{' || 
        '"name": "' || case_name || '",' ||
        '"location_type_code": "organization",' || --provider
        '"parent_location_id": "' || org.location_id || '",' || 
        '"site_code": "' || LOC_ID || '"' ||
        '}'
    ) payload
    from case_provider p 
        left join ld on ld.p_id = p.external_id
        left join locs on locs.site_code = p.external_id
        left join locs org on org.site_code = 'facility_registry'
    where ld.p_id is null and locs.location_id is null and loc_id is not null
    and closed=false
)
,clin as (
    select distinct
    c.external_id LOC_ID, 
    parse_json(
        '{' || 
        '"name": "' || ifnull(c.case_name, '') || '",' ||
        case when c.latitude is not null then '"latitude": "' || c.latitude || '",' else '' end || 
        case when c.longitude is not null then '"longitude": "' || c.longitude || '",' else '' end || 
        '"location_type_code": "facility",' || --clinic
        '"parent_location_id": "' || ifnull(locs.location_id, '') || '",' || 
        '"site_code": "' || ifnull(LOC_ID, '') || '"' ||
        '}'
    ) payload
    from case_clinic c 
        left join ld on ld.p_id || '-' || ld.a_id = c.external_id
        left join locs on locs.site_code = split_part(c.external_id, '-', 1)
                and locs.location_type_code = 'organization'
        left join locs fac on fac.site_code = c.external_id
                and fac.location_type_code = 'facility'
    where ld.p_id is null and fac.location_id is null 
        and c.external_id is not null and locs.location_id is not null
        and closed=false
)
,clin_data as (
    select distinct
    c.external_id || 'facility_data' LOC_ID, 
    parse_json(
        '{' || 
        '"name": "' || regexp_replace(replace(lower(c.case_name), ' ', '_'), '[^a-z0-9|_]+', '') || '_data",' ||
        '"location_type_code": "facility_data",' || --clinic
        '"parent_location_id": "' || locs.location_id || '",' || 
        '"site_code": "' || LOC_ID || '"' ||
        '}'
    ) payload
    from case_clinic c 
        left join locs on locs.site_code = c.external_id
                and locs.location_type_code = 'facility'
        left join locs fd on fd.site_code = c.external_id || 'facility_data'
                and fd.location_type_code = 'facility_data'
        left join ld on ld.p_id || '-' || ld.a_id = c.external_id
    where ld.p_id is null and fd.location_id is null 
        and c.external_id is not null and locs.location_id is not null
        and closed=false
),
-- ********** END REMOVE SECTION **********
org_payloads as (
    SELECT distinct
    p_id LOC_ID, 
    parse_json(
        '{' || 
        case when org.location_id is not null then '"location_id": "' || org.location_id || '",' else '' end || 
        '"name": "' || BHA_GENERAL_ACCT || '",' ||
        '"location_type_code": "organization",' || --provider
        '"parent_location_id": "' || locs.location_id || '",' || 
        '"site_code": "' || LOC_ID || '"' ||
        '}'
    ) payload
    from ld
        left join locs on site_code = 'facility_registry'
        left join locs org on org.location_type_code = 'organization' and 
                org.site_code = p_id
    where (org.location_id is null or (org.name != ld.BHA_GENERAL_ACCT or locs.location_id != org.parent_location_id))
        and locs.location_id is not null
),
fac_payloads as (
    SELECT distinct
    p_id || '-' || a_id LOC_ID, 
    parse_json(
        '{' || 
        case when fac.location_id is not null then '"location_id": "' || fac.location_id || '",' else '' end || 
        '"name": "' || ld.account_name || '",' ||
        case when ld.latitude is not null then '"latitude": "' || ld.latitude || '",' else '' end || 
        case when ld.longitude is not null then '"longitude": "' || ld.longitude || '",' else '' end || 
        '"location_type_code": "facility",' || --clinic
        '"parent_location_id": "' || locs.location_id || '",' || 
        '"site_code": "' || LOC_ID || '"' ||
        '}'
    ) payload
    from ld
        left join locs on locs.site_code = p_id
                and locs.location_type_code = 'organization'
        -- changed by slu - facility site_code: parent_account_id, "#", account_id
        left join locs fac on fac.site_code = p_id || '-' || a_id
                and fac.location_type_code = 'facility'
    where (fac.location_id is null or (fac.name != ld.account_name or locs.location_id != fac.parent_location_id
            or fac.latitude::number(10,7) != ld.latitude::number(10,7) or fac.longitude::number(10,7) != ld.longitude::number(10,7)))
        and locs.location_id is not null
        -- temporary changes to skip account name = [LEGACY ACCOUNT] Colorado Mental Health Institute - General Account
        and account_name <> 'Atlas Counseling & Consulting, PLLC'
        -- end temporary        
),
fac_data_payloads as (
    SELECT distinct
    p_id || '-' || a_id || 'facility_data' LOC_ID, 
    parse_json(
        '{' || 
        case when fd.location_id is not null then '"location_id": "' || fd.location_id || '",' else '' end || 
        '"name": "' || regexp_replace(replace(lower(ld.account_name), ' ', '_'), '[^a-z0-9|_]+', '') || '_data",' ||
        '"location_type_code": "facility_data",' || --clinic
        '"parent_location_id": "' || locs.location_id || '",' || 
        '"site_code": "' || LOC_ID || '"' ||
        '}'
    ) payload
    from ld
        left join locs on locs.site_code = p_id || '-' || a_id
                and locs.location_type_code = 'facility'
        left join locs fd on fd.site_code = p_id || '-' || a_id || 'facility_data'
                and fd.location_type_code = 'facility_data'
    where (fd.location_id is null or (fd.name != regexp_replace(replace(lower(ld.account_name), ' ', '_'), '[^a-z0-9|_]+', '') || '_data' or locs.location_id != fd.parent_location_id))
        and locs.location_id is not null
),
payloads as (
    select * from org_payloads
    union 
    select * from fac_payloads
    union 
    select * from fac_data_payloads
	-- ********** REMOVE SECTION WHEN NO LONGER NEEDED **********
    union 
    select * from prov
    union 
    select * from clin
    union 
    select * from clin_data
	-- ********** END REMOVE SECTION **********
),
numbered_payloads as ( --add row numbers and groupings of 100 at a time
    select row_number() over(order by LOC_ID asc) rownum, ceil(rownum/100) grouping, payload payloads from payloads
),
final as (
select grouping, '{"objects": [' || listagg(payloads::string, ',') || ']}' payload --concatenate payloads into arrays of up to 100
from numbered_payloads group by 1 having payload <> '{"objects": []}' order by 1
)

select 
	GROUPING,
	PAYLOAD
from final
