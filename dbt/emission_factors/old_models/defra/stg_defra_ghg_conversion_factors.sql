/*
  Stage the DEFRA 2022 dataset 
*/
with
    defra_22 as (
        select
            2022 as year,
            substring("Scope" from 'Scope ([1-3])')::int as scope,
            "Level 1" as level_1,
            "Level 2" as level_2,
            "Level 3" as level_3,
            "Level 4" as level_4,
            "Column Text" as comment,
            "UOM" as unit,
            "GHG/kWh" as ghg_or_kwh,
            "GHG Conversion Factor 2022"::float8 as ghg_cf
        from {{ source("ref_data", "defra_cf_2022") }}
        /* remove non numeric value (like (<1)) */
        where "GHG Conversion Factor 2022" ~ '^\d+(\.\d+)?$'
    )
select *
from defra_22
