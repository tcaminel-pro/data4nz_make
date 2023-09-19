/*  
Table: opdata_map_with_ef
    Compute impact factors for each row in the operation data table.
    The table is named in 'operational_data' dbt variable
    Calculate the emission factors associated to the row, and weighted_value 
    with coeficients givent in the operational table.

    Copyright (C) 2023 Eviden. All rights reserved
*/

with
    step_1 as (
        /* join the opdata with the 'ef_merge_items' table. 
        The jointure can be done either on the id, if it's given, or with the name
        The conditional join trick is explained here:  https://stackoverflow.com/questions/10256848/can-i-use-case-statement-in-a-join-condition
        */
        select
            job_id,
            opdata.op_name,
            ef.id,
            ef.item,
            product_unit,
            dataset_id,
            area as request_area,
            coalesce(factor, 1.0) as f1,
            descr as item_desc
        -- from {{ var("operational_data") }} as opdata
        from {{ ref("ef_requests") }} as opdata
        left join {{ ref("ef_merged_items") }} as ef 
            on case
                when opdata.db_id is not null and opdata.db_id = ef.id then 1
                when opdata.op_name is not null and lower(opdata.db_item) = lower(ef.item) then 1
                else 0
            end = 1
        
    ),
    step_2 as (
        /* join with the EF values */
        select step_1.*, area, emission_factor, countries, value_ef
        from step_1
        left join {{ ref("ef_merged_values") }} as vals on step_1.id = vals.id
    ),
    step_3 as (
        /* calculate the weighted_value, and rank the geographical matching from 0 to 10:
            best if exact match on the country
            then if the country requested is in the country list
            then if the country in the database is 'Global', 
            and last  if it's 'Rest f the World'
        */
        select step_2.*, 
            value_ef * f1  as weighted_value, 
            case 
                when request_area = countries then 10  -- exact match ; most precise EF
                when request_area like '%' || countries || '%' then 8 -- In geo area, that's OK                
                when countries = 'GLO' then 6 -- we have a EF at Global, that's fine
                when request_area = 'GLO' and countries = 'RoW' then 4  -- Global is requested  ((maybe invert wih above ? )
                when request_area = 'GLO' then 3  -- Global is requested  ((maybe invert wih above ? )
                when countries = 'RoW' then 1  -- Rest of the world 
                else 0
            end as geo_rank
        from step_2
    ), 
    step_4 as (
        /* add a  column with the dataset */
        select step_3.*, concat(provider, ' [', version, ']') as dataset
        from step_3 
        left join {{ref("datasets")}} on  datasets.id = step_3.dataset_id
    )
select * from step_4
order by op_name