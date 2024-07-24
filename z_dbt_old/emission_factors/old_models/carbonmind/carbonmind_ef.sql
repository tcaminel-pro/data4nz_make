with
    stg as (select * from {{ ref("stg_carbonmind") }}),
        
    ef_row as (
        select uuid, country_code, global_gwp as value_ef, 'GWP' as emission_factor
        from stg
        union
        select uuid, country_code, biogenic_gwp as value_ef, 'GWPb' as emission_factor
        from stg
        union
        select uuid, country_code, fossil_gwp as value_ef, 'GWPf' as emission_factor
        from stg
        union
        select uuid, country_code, land_use_gwp as value_ef, 'GWPlu' as emission_factor
        from stg
    ),
    final as (
        select
            stg.uuid as id,
            emission_factor,
            coalesce (stg.country_code, 'GLO') as countries,
            coalesce (stg.country_code, 'GLO') as area,  -- TODO: create array of countries
            value_ef,
			null as meta
        from ef_row
        inner join stg on ef_row.uuid = stg.uuid
    )
select *
from final