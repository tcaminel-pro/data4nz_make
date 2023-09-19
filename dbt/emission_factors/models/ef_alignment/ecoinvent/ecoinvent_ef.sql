/* 
  Align the EcoInvent dataset values with the shared scheme for all EF  
  The 'countries' field contains the list of countries (not regions)
  The original geo name is put in the 'meta' field, as wel a the  Excel column id 

  Copyright (C) 2023 Eviden. All rights reserved
*/
--fmt: off
with
    step_1 as (
        /* get normalized value for the emission factor */
        select
            uuids as id,
            emission_factor,
            geography as geo,
            value_ef,
            product_amount,
            json_build_object('xls', excel_column) as meta
        from {{ ref("ecoinvent_rows_with_calculated") }} as ei_stg
        left join {{ ref("emission_factors") }} as ef
            on ei_stg.category like concat(ef.category, '%')
    ),

    step_2 as (
        /* get the list of countries associated to a geography
       (we use the fact that lenght of country is 2 bytes)
       'RoW' and 'GLO' are kept as-is, and when there are no geo we consider it's global
     
     TODO : move that after the big join ?
     */
        select
            step_1.*,
            case
                when contained_countries is not null
                then array_to_string(contained_countries, ',')
                when length(geo) = 2 or geo = 'RoW' or geo = 'GLO'
                then geo
                when geo is null
                then 'GLO'
            end as countries
        from step_1
        left join
            {{ ref("contained_countries") }}
            on step_1.geo = contained_countries.short_name
    ),
    step_3 as (
        /* get the full name of the geography */
        select step_2.*, name as country_name
        from step_2
        left join
            {{ ref("stg_ecoinvent_geography") }}
            on step_2.geo = stg_ecoinvent_geography.short_name
    ),
    final as (
        select
            id,
            emission_factor,
            case when country_name is null then geo else country_name end as area,
            countries,
            value_ef,   -- TODO : consider multiply by product_amount   (ie -1 if waste)
            meta
        from step_3
        where countries is not null
    )
select *
from final
