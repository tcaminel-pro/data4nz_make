 /* 
  Align the DEFRA dataset with the shared scheme
  Infortunatly there a little description.
  
  Copyright (C) 2023 Eviden. All rights reserved
*/
select
    6 as dataset_id,
    {{
        dbt_utils.generate_surrogate_key(
            ["level_1", "level_2", "level_3", "level_4", "unit", "comment"]
        )
    }} as id,
    concat(level_2, ' - ', level_3, ' - ', level_4, ' - ', comment) as item,
    unit as product_unit,
    json_strip_nulls(json_build_object('l1', level_1, 'l2', level_2)) as description
from {{ ref("stg_defra_ghg_conversion_factors") }}
where ghg_or_kwh = 'kg CO2e'
