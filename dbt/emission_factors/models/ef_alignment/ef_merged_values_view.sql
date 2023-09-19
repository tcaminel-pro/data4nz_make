with
    ef_values as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ecoinvent_ef"),
                    ref("negaoctet_ef"),
                    ref("carbonmind_ef"),
                    ref("test_ef_values"),
                
                
                ],
                source_column_name="None",
            )
        }}
    ),
    ef_def as (select emission_factor, unit from {{ ref("emission_factors") }}),

    final as (
        select
            id, ef_def.emission_factor, unit, area, countries, cast(value_ef as float4), meta
        from ef_values
        join ef_def on ef_values.emission_factor = ef_def.emission_factor
    )

select *
from final
