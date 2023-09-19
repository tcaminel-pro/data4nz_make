/*
Calculate scope 1,2,3 emissions from CDP questions.

For scope 1 we take the row 'Reporting year' of column 'Gross global Scope 1 emissions (metric tons CO2e)'
Same for scope 2 with columns 'C6.3','Scope 2, location-based' and 'C6.3', 'market-based (if applicable)
Scope 3 is the sum of column 'Metric tonnes CO2e'

Copyright (C) 2023 Eviden. All rights reserved
*/

{#
{{ config(
    materialized = 'table',
    indexes=[{'columns': ['organization']}, {'columns': ['year']} ]
)}}
#}

with 
scope_1_cte as ( {{cdp_value_in_row(
    'C6.1',  'Gross global Scope 1 emissions (metric tons CO2e)', 'Reporting year')}}), 
scope_2_lb_cte as ( {{cdp_value_in_row(
    'C6.3','Scope 2, location-based', 'Reporting year' )}} ), 
scope_2_mb_cte as ({{cdp_value_in_row(
    'C6.3', 'Scope 2, market-based (if applicable)','Reporting year' )}} ), 
scope_3_cte as (select organization, year, sum(value_num) as value_num, null as value_str
    from {{ ref("stg_q_and_a_rows") }}
    where question = 'Metric tonnes CO2e' and sheet = 'C6.5'
    group by organization, year),
union_cte as
            ( select 'scope_1' as question, * from scope_1_cte 
    union     select 'scope_2_lb' as question, * from scope_2_lb_cte 
    union     select 'scope_2_mb' as question, * from scope_2_mb_cte 
    union     select 'scope_3' as question, * from scope_3_cte  )

select '_Calculated' as sheet, * from union_cte
group by year, organization,sheet, question, value_num, value_str
order by organization,year
