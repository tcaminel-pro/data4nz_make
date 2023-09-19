/* 
Calculate the average EF of electricity for countries where EI does have only values for their regions.
(like USA, India, ...)

We create a view similar to stg_ecoinvent, so we can do a UNION later
  
Copyright (C) 2023 Eviden. All rights reserved
*/



with 
selection as (
	SELECT  substring(geography,1,2) as country,category, value_ef
	FROM {{ref("stg_ecoinvent")}} 
	where activity_name ='market for electricity, low voltage'
	and length(geography) > 2 ),
 average as (
	select country,category, avg(value_ef) as value_ef, ROW_NUMBER () OVER () as row_number
	from selection
	group by country, category
	),
stg_like as (
   SELECT 
		'calculated_row' as uuids , 
		value_ef, 
		'' as excel_column, 
		category, 
		'calculated' as activity_uuid, 
		concat('calc', row_number) as a_id, 
		'd69294d7-8d64-4915-a896-9996a014c410' as p_id, 
		'market for electricity, low voltage' as activity_name, 
		country as geography, 
		'electricity, low voltage' as product_name, 
		'kWh' as product_unit,
		1 as product_amount
	from 	average
)
select * from stg_like	