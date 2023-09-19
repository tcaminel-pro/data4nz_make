/*
Get GDP (Gross domestic product) from OECD data, extracted from:
https://stats.oecd.org/index.aspx?queryid=60702 
(in 03/2023)
*/

SELECT
    "Country" AS country,
    "LOCATION" as country_code,
    "Year" as year, 
    "Unit Code" as currency_code, 
    "Unit" as currency_unit, 
    "Value" * Power (10, "PowerCode Code") /1000000  as gdp_in_million
FROM {{ source('ref_data', 'oecd_gdp_table_08_2022') }}
WHERE "Transaction" = 'Gross domestic product (output approach)'

