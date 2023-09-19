/*  Create a table from different emission factors datasets, with their ef_values
    The text fields are indexed for full text search
*/
{{ config(materialized="table", indexes=[{"columns": ["id"]}]) }}

select id, emission_factor, unit, area, countries, cast(value_ef as float4), meta
from {{ref("ef_merged_values_view")}}
