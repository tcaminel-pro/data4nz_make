WITH
  cdp_isin as ( SELECT * from {{ref('map_cdp_isin_factcheck')}} ),
  isin_lei as (SELECT 
     "ISIN"  AS isin,
     "LEI" as lei 
     FROM {{source('ref_data', "ISIN_LEI_0822")}})
SELECT
   cdp_organization, cdp_isin.isin, factset_entity_id, lei
FROM cdp_isin 
    JOIN isin_lei ON  cdp_isin.isin =  isin_lei.isin