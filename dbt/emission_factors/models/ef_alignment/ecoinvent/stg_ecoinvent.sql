/* 
Stage EcoAct data tables as created by the 'ecoinvent_to_sql' Python script

TODO : Conider refactoring it. 
  We could simplify as we have now a separation netween description and values tables
  But on the other side, it's just views so the SQL optimizer shoud work fine.
*/
/* get values for EF 3.0 method */
/* EF 3.0 code_id*/

-- fmt: off
with 
  ef_values_cte as (
    select 
      uuids,
      value as value_ef, 
      excel_column, 
      category 
    from ref_data."EI3.9_Values" as values
    left join {{ source("ref_data", "EI3.9_Categories") }} as categories
    on categories.index = values.category_id
    where method_id = 0
    )   , 

  activities_cte as (
    select
      uuids as activity_uuid,
      split_part(uuids, '_', 1) as a_id,
      split_part(uuids, '_', 2) as p_id,
      "Activity Name" as activity_name,
      "Geography" as geography,
      "Reference Product Name" as product_name,
      "Reference Product Unit" as product_unit,
      "Reference Product Amount" as product_amount
  from {{ source("ref_data", "EI3.9_Activities") }} as activities )

select * from ef_values_cte
inner join activities_cte on activities_cte.activity_uuid = ef_values_cte.uuids 

/* TODO :  replace split_part  by dbt macro */
/* inner join ef_values on uuids = ef_values.uuids */
