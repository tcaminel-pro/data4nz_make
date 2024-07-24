/*
 Most recent country GCP, in dollar
*/

{{ config(materialized="table") }}

WITH cte AS
(
   SELECT *, 
          ROW_NUMBER() OVER (PARTITION BY country ORDER BY year DESC) AS row_number
   FROM {{ref('stg_oecd_gdp')}}
   WHERE  currency_code = 'USD'
)
SELECT country, country_code, year, gdp_in_million as gdp_in_million_dollar
FROM cte
WHERE row_number = 1

/* 
Inspired from https://dba.stackexchange.com/questions/305795/how-to-retrieve-the-maximum-value-and-its-corresponding-date-in-a-table 
*/
