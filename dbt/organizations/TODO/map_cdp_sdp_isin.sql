WITH
  cdp as (SELECT organization as cdp_name, isin from {{ref('cdp_company_info')}} ),
  sbt as (SELECT company_name as sbt_name, isin from {{ref('stg_sbt_companies')}} )
SELECT
   cdp.isin, cdp_name, sbt_name
FROM cdp 
    FULL JOIN sbt ON  cdp.isin =  sbt.isin