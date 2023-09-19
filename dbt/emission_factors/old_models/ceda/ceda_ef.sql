/*
select the EF values from the CEDA database. 

Today take just the GWP (CO2). 
TODO: take all.
TODO: Align geo
*/
with
    desc_cde as (select * from {{ ref("stg_ceda_description") }}),
    val_cde as (select * from {{ ref("stg_ceda_us") }})
select distinct
    concat(desc_cde.id, '_', {{ dbt_utils.generate_surrogate_key(["item"]) }}) as id,
    'GWP' as emission_factor,
    'United States' as area,
    'US' as countries,
    scope_123 as value_ef,
    null as meta
from desc_cde
inner join val_cde on desc_cde.id = val_cde.id
