/*
   Get contained countries for ecoinvent geographies.

   We start from the ecoinvent_geography table that contains  a list of 
   countries and other geographies for geographies used in the database, 
   and then find recursively the countries contained in these geographies.

   In fact, we just go toward a level of 3 - that looks enough.

   Copyright (C) 2023 Eviden. All rights reserved    
*/

with 
l1_cte as ( 
	select short_name,  unnest(contained_geographies) AS contained
	from  {{ ref("stg_ecoinvent_geography") }} ),
l2_cte as (
	select distinct  l2.short_name, l2.contained 
	from  l1_cte left join l1_cte as l2 on l2.short_name = l1_cte.short_name),
l3_cte as (
	select distinct l3.short_name, l3.contained 
	from l2_cte join l1_cte as l3 on l3.short_name = l2_cte.short_name),	  
union_cte as (
	select * from l1_cte  union all 
	select * from l2_cte  union all 
	select * from l3_cte),
country_cte as ( 
	select distinct  short_name, trim(contained) as country
	from union_cte
	where length(trim(contained))=2 
	),
group_cte as (
	select short_name, array_agg(country order by country) as contained_countries
	from country_cte
	group by short_name) 
select * from group_cte



