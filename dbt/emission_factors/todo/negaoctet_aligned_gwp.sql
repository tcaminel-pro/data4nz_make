select
    {{
        dbt_utils.star(
            from=ref("negaoctet_aligned"), except=["emission_factor", "value_ef"]
        )
    }}, value_ef as "GWP"
from {{ ref("negaoctet_aligned") }}
where emission_factor = 'GWP'
