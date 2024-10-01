
/*
    Identify clinics with open Template and non-template/real unit cases.
    Close the Template unit cases.
*/
with
dm_table_data_capacity as (
      select * from {{ source('dm_table_data', 'CASE_CAPACITY') }} where closed=false
)
, dm_table_data_unit as (
      select * from {{ source('dm_table_data', 'CASE_UNIT') }} where closed=false
)
,cte_parent_with_template_and_real_unit as (
    select 
        parent_case_id,
        sum(case when coalesce(case_name, '') = 'Template Unit 1' then 1 else 0 end) as template_unit_count,
        sum(case when coalesce(case_name, '')  <> 'Template Unit 1' then 1 else 0 end) as real_unit_count,
    from  dm_table_data_unit
    group by parent_case_id
    having 
        real_unit_count >= 1 
        and template_unit_count >= 1
)
, cte_template_units as (
    select case_name, case_id, parent_case_id, owner_id, case_type, external_id
    from dm_table_data_unit 
        where coalesce(case_name, '') = 'Template Unit 1'
)
, cte_template_units_to_close as ( 
    select 
        cte_template_units.*,
    from cte_template_units 
        join cte_parent_with_template_and_real_unit  
        where cte_template_units.parent_case_id = cte_parent_with_template_and_real_unit.parent_case_id
)
,final as (
    select 
        to_close_unit.*, 
        capacity.case_id as capacity_case_id,
        capacity.case_name as capacity_case_name,
        capacity.external_id as capacity_external_id,
        capacity.case_type as capacity_case_type,
        capacity.owner_id as capacity_owner_id
    from cte_template_units_to_close to_close_unit
        left join dm_table_data_capacity capacity
        on capacity.unit_case_ids = to_close_unit.case_id
    where capacity.case_name = 'Template Bed Group 1'
)
select * from final