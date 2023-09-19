/* 
  Align the DEFRA dataset with the shared scheme.

  We use an hashcode to generate an uniqu id, as there's none in the Excel file
  
  
  Today GWP only.
  TODO : take other emission factors
  TODO: Align geo

  Copyright (C) 2023 Eviden. All rights reserved
*/
select
    {{
        dbt_utils.generate_surrogate_key(
            ["level_1", "level_2", "level_3", "level_4", "unit", "comment"]
        )
    }} as id,
    'GWP' as emission_factor,
    'United Kingdom' as aera,
    'GB' as countries,
    ghg_cf as value_ef,
    null as meta
from {{ ref("stg_defra_ghg_conversion_factors") }}
where "ghg_or_kwh" = 'kg CO2e' and ghg_cf is not null
