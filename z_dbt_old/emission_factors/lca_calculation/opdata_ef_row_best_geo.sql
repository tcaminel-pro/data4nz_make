/*
Table : opdata_ef_row_best_geo
Extend returned information with best geographical match, 
and additional information

Copyright (C) 2023 Eviden. All rights reserved
*/

-- fmt: off
with 
best_geo_rank as (
    -- find the best ranking for geo area match
    select op_name as best_item, max(geo_rank) as best_geo_rank
    from {{ ref("opdata_ef_row") }}
    group by op_name
), 
item_desc as (
    -- select extra data
    select id as item_id, descr, cas, isic, proxies
    from {{ref("ef_merged_items")}}
),
final as (
    -- join 
    select {{ dbt_utils.star(ref("opdata_ef_row")) }}, descr, cas, isic,proxies 
    from {{ ref("opdata_ef_row") }}  as op
    inner join best_geo_rank  as best
        on op.op_name = best.best_item and op.geo_rank = best.best_geo_rank
    inner join item_desc
        on item_desc.item_id = op.id
) 
select  * from final