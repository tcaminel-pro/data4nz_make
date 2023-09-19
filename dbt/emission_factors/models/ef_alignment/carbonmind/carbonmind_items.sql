/* 
Align the CarbonMind Dataset

We combinconcant the flow_name and the process_name to have an activity

Note: The rexep is to remove the country in process_name. BUT, llthought it woks quite well, 
it also remove relevant information in some rare cases - for example in 'oligomerization of ethylene (Chevron)' 
TODO: Remove only contries (in Python, or using the stg_ecoinvent_geography table)
*/
with
    stg as (select * from {{ ref("stg_carbonmind") }}),

    final as (
        select
            'CM22' as dataset_id,  -- TODO: not hardcoded 
            stg.uuid as id,
            concat(flow_name, ' / ', 
                regexp_replace( process_name, '\([A-Z].{3,}\)$', '') ) as item,
            'kg' as product_unit,
            json_strip_nulls(
                json_build_object(
                    'cas', cas, 'descr', process_description, 'process_name',process_name
                )
            ) as description

        from stg
    )
select *
from final
