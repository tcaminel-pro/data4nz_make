/*
  Select EF velues from NegaOctet database.
  TODO: import other EF collected by EcoAct 

*/
select
    manuf_bilan as id,
    emission_factor,
    'Global' as area,
    'GLO' as countries,
    sum(value_ef) as value_ef,
    null as meta
from {{ ref("stg_negaoctet") }} as negaoctet
left join
    {{ ref("stg_negaoctet_normalize_ef") }}
    on (negaoctet.indicator = stg_negaoctet_normalize_ef.negaoctet_indicator)

--fmt: off
where value_ef is not null 
{{ dbt_utils.group_by(n=4) }}
