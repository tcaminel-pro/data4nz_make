/*
  Stage the Negaotte tables, as created by the 'negaocted_to_sql'
  Python script.  
*/
with
    products_cte as (
        select
            manuf_bilan,
            "Level" as level,
            "TIER" as tier,
            "Category 1" as category_1,
            "Category 2" as category_2,
            "Category 3" as category_3,
            "End of life ecobilan" as eol_ecobilan,
            "Name" as name,
            "Additional information" as additional_info,
            "Unit" as product_unit,
            "Transport scenario" as transport_scenario,
            "Country of use" as country_of_use,
            "Annual electricity consumption (kWh)" as annual_elec_cons_kwh,
            "Mass (kg)" as mass_kg
        from {{ source("ref_data", "NO22_Products") }}
    ),
    value_cte as (
        select *
        from ref_data."NO22_Values" as
        values
        inner join
            ref_data."NO22_Categories" as categories
            on (values.category_id = categories.index)
        inner join
            ref_data."NO22_Indicators" as indicators
            on (values.indicator_id = indicators.index)
        inner join ref_data."NO22_Units" as units on (values.unit_id = units.index)
    )
select products_cte.*, category, value_cte.indicator, value_cte.unit, value as value_ef
from products_cte
inner join value_cte on products_cte.manuf_bilan = value_cte.manuf_bilan
