select *
from {{ ref("negaoctet_aligned") }} as negaoctet
where emission_factor != '??' and value_ef is not null


union
select *
from {{ ref("ecoinvent_aligned") }} as ecoinvent
where emission_factor != '??' and value_ef is not null
