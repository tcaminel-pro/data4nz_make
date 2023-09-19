
{{ config(materialized="table") }}

select distinct  
   p_id,
   product_name,
   cpc_classification,
   unit,
   product_information,
   cas_number,
   cut_off_classification ,
   ARRAY_AGG(distinct epa.activity_name) as activities

from {{ ref("stg_ecoinvent_en15804_isic") }}
inner join {{ ref("ecoinvent_products_activities") }} as epa  using (product_name)
{{ dbt_utils.group_by(n=7) }}

