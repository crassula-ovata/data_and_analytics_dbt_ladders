with locs as (
    select * from  {{ source('dm_table_data', 'LOCATION') }}
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
        -- temporary changes to skip account name = [LEGACY ACCOUNT] Colorado Mental Health Institute - General Account
        and ld.bha_general_acct <> '[LEGACY ACCOUNT] Colorado Mental Health Institute - General Account'
        and ld.bha_general_acct <> '[LEGACY SITE ACCOUNT] Jefferson Center for Mental Health - Independence'
        -- end temporary        
),
fac_payloads as (
    SELECT distinct
    p_id || '-' || a_id LOC_ID, 
    parse_json(
        '{' || 
        case when fac.location_id is not null then '"location_id": "' || fac.location_id || '",' else '' end || 
        '"name": "' || ld.account_name || '",' ||
        '"latitude": "' || ld.latitude || '",' ||
        '"longitude": "' || ld.longitude || '",' ||
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
        --and account_name <> 'Choice House PHP Site'
        and account_name <> '[LEGACY SITE ACCOUNT] Jefferson Center for Mental Health - Independence'
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
        -- temporary changes to skip account name = [LEGACY ACCOUNT] Colorado Mental Health Institute - General Account
        and ld.account_name <> '[LEGACY ACCOUNT] Colorado Mental Health Institute - General Account'
        and ld.account_name <> '[LEGACY SITE ACCOUNT] Jefferson Center for Mental Health - Independence'
        -- end temporary
),
payloads as (
    select * from org_payloads
    union 
    select * from fac_payloads
    union 
    select * from fac_data_payloads
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
