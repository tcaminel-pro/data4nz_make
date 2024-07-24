/*
Join the CEDA data base ids and the descripions, 
and align wth the shared scheme  

The id is the CEDA Code + the MD5, because there a few duplicated CEDA Code 
*/
with
    desc_cde as (select * from {{ ref("stg_ceda_description") }}),
    val_cde as (select * from {{ ref("stg_ceda_us") }})
select
    5 as dataset_id,
    concat(desc_cde.id, '_', {{ dbt_utils.generate_surrogate_key(["item"]) }}) as id,
    item,
    unit as product_unit,
    json_strip_nulls(json_build_object('dsc', description)) as description
from desc_cde
inner join val_cde on desc_cde.id = val_cde.id
