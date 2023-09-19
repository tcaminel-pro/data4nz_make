/*
Table : opdata_ef_row
 Pivot view of model 'opdata_map_with_ef' with emission factors as column

 Copyright (C) 2023 Eviden. All rights reserved
*/
select
    job_id,
    op_name,
    item,
    id,
    dataset,
    product_unit,
    area,
    geo_rank,
    {{
        dbt_utils.pivot(
            column="emission_factor",
            values=dbt_utils.get_column_values(
                table=ref("emission_factors"), column="emission_factor"
            ),
            then_value="value_ef",
        )
    }}
-- fmt: off
from {{ ref("opdata_map_with_ef") }}
{{ dbt_utils.group_by(n=8) }}
/* GROUP BY name, transformation, product_unit */
order by op_name
