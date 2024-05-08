with
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
hades_table_data_ladders_active_licenses as (
      select * from  {{ source('hades_table_data', 'VWS_LADDERS_ACTIVE_LICENSES') }}
), 
locs as (
      select * from  {{ source('dm_table_data', 'LOCATION') }}
),
p_share as (
    select 
        distinct
        dm.external_id_format (parent_account_id) as parent_account_id,
        bha_general_acct 
    from hades_table_data_ladders_active_licenses
 --   where parent_account_id = '0014m_00001hh_zzl_q_a_s_'
        order by BHA_GENERAL_ACCT asc
),

p_share_otp as (
    select 
        dm.external_id_format (parent_account_id) as parent_account_id,
        listagg(opioid_treatment_programs) as opioid_treatment_provider_agg,
        contains(opioid_treatment_provider_agg, 'true') as opioid_treatment_provider
        
     from hades_table_data_ladders_active_licenses --where parent_account_id = '0016100001h_a_07x_a_a_t_'
     group by parent_account_id

),

p_prod as (
   
    select * from (
     select 
        distinct
        case_id, 
        trim(external_id) as external_id, 
        owner_id,
        date_opened, 
        case_name,
        rank () over ( partition by external_id order by date_opened asc ) as date_rank_p,
        opioid_treatment_provider
        
     from dm_table_data_provider p1 where closed = 'FALSE' and external_id is not null) where date_rank_p = 1 
),
-- 05/06/2024 p_share_union and p_share_union_otp will be used for additional data check between p_share and p_prod (with p_share is the primary data source)
-- that if a case is not synced between p_share and p_prod, data will be retained between these two data sources
p_share_union as (
	select * 
	from (
		select parent_account_id, bha_general_acct, 1 as priority from p_share
		union
		select external_id parent_account_id, case_name bha_general_acct, 2 as priority from p_prod
	)
	qualify row_number() over (partition by parent_account_id order by priority asc) = 1
)
,p_share_union_otp as (
    select *
    from (
        select parent_account_id, opioid_treatment_provider, 1 as priority from p_share_otp
        union
        select external_id parent_account_id, opioid_treatment_provider, 2 as priority from p_prod
    )
    qualify row_number() over (partition by parent_account_id order by priority asc) = 1
)
,new_providers as (

    select 
        null as n_date_opened,
        p_prod.case_id,
        p_share.parent_account_id as external_id,
        locs.location_id as owner_id,
        'provider' as case_type,
        p_share.bha_general_acct as case_name,
        p_share_otp.opioid_treatment_provider,
        null as update_name_action, 
        null as update_otp_action,
        null as update_owner_action,
        case when p_prod.external_id is null then 'create' else null end as action,
    current_timestamp() as import_date
    from p_share left join p_prod on p_share.parent_account_id = p_prod.external_id 
                 left join p_share_otp on p_share_otp.parent_account_id = p_share.parent_account_id
                 left join locs on locs.site_code = p_share.parent_account_id
    where action = 'create'
    order by case_name
), 

updated_providers as (

    select 
        p_prod.date_opened as u_date_opened,
        p_prod.case_id,
        p_prod.external_id,
        locs.location_id as owner_id,
        'provider' as case_type,
        p_share_union.bha_general_acct as case_name,
        iff(p_share_union_otp.opioid_treatment_provider = p_prod.opioid_treatment_provider, p_prod.opioid_treatment_provider,                                             p_share_union_otp.opioid_treatment_provider) as opioid_treatment_provider,
        iff(p_share_union.parent_account_id = p_prod.external_id and 
            p_share_union.bha_general_acct <> p_prod.case_name, 'update_name', null) as name_action,
        iff(p_share_union.parent_account_id = p_prod.external_id and 
            nvl(p_share_union_otp.opioid_treatment_provider::string,'') <> nvl(p_prod.opioid_treatment_provider::string, ''), 
            'update_otp', null) as otp_action,
        iff(p_share_union.parent_account_id = p_prod.external_id and 
            nvl(p_prod.owner_id,'') <> nvl(locs.location_id, ''), 'update_owner', null) as owner_action,
        iff(name_action = 'update_name' or otp_action = 'update_otp' or owner_action = 'update_owner', 'update', null) as action,
    current_timestamp() as import_date
    from p_share_union left join p_prod on  p_share_union.parent_account_id = p_prod.external_id
                 left join p_share_union_otp on p_share_union_otp.parent_account_id = p_share_union.parent_account_id
                 left join locs on locs.site_code = p_share_union.parent_account_id
    where action like 'update' 
    order by case_name
),

final as (
select * from (select n_date_opened,case_id, external_id, owner_id, case_type, case_name, 
               case opioid_treatment_provider
                when TRUE then 'yes'
                when FALSE then 'no'
               else null end as opioid_treatment_provider, 
               update_name_action, update_otp_action, update_owner_action, action, import_date 
               from new_providers 
               union 
               select u_date_opened,case_id, external_id, owner_id, case_type, case_name,
               case opioid_treatment_provider
                when TRUE then 'yes'
                when FALSE then 'no'
               else null end as opioid_treatment_provider, name_action, otp_action, owner_action, action, import_date
               from updated_providers)

order by action, external_id
)

select 
	N_DATE_OPENED,
	CASE_ID,
	EXTERNAL_ID,
	OWNER_ID,
	CASE_TYPE,
	CASE_NAME,
	OPIOID_TREATMENT_PROVIDER,
	UPDATE_NAME_ACTION,
	UPDATE_OTP_ACTION,
    UPDATE_OWNER_ACTION,
	ACTION,
	IMPORT_DATE
from final