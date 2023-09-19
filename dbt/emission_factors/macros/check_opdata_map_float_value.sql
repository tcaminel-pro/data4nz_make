/*
*/
{% macro check_opdata_map_float_value(where, expected_value) %}

with
    query as (
        select value_ef
        from {{ ref("opdata_map_with_ef") }}
        where {{ where }}
    ),
    expected as (select {{expected_value}} as expected_value)
select expected_value, value_ef, abs(expected_value - value_ef)::float4 as delta
from expected, query

{% endmacro %}


