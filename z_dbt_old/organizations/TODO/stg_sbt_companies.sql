/* 
SELECT
    "Company Name" AS  company_name ,
    "ISIN" AS  isin ,
    "Near term - Target Status" AS  near_term_target_status ,
    "Near term - Target Classification" AS  near_term_target_classification ,
    REPLACE("Near term - Target Year", 'FY', '' )  AS  near_term_target_year ,
    "Long term - Target Status" AS  long_term_target_status ,
    "Long term - Target Classification" AS  long_term_target_classification ,
    REPLACE("Long term - Target Year", 'FY', '' ) AS  long_term_target_year ,
    CAST("Net-Zero Committed" AS boolean) AS  net_zero_committed ,
    "Net-Zero Year" AS  net_zero_year ,
    "Organization Type" AS  organization_type ,
    CAST("BA1.5?" AS boolean) AS  is_ba1point5 ,
    CAST("BA1.5 Date" as date) AS  ba1point5_date ,
    "Country" AS  country ,
    "Region" AS  region ,
    "Sector" AS  sector ,
    CAST("Date" AS date) AS  date ,
    "Target" AS  target ,
    "Target Classification" AS  target_classification ,
    "Extension" AS  extension
FROM {{ source('ref_data', 'sbt_compagnies_actions_2021') }}

*/