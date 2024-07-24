WITH 
    cet AS  ( SELECT
        "Company Name" AS cdp_organization,
        "ISIN" AS isin,
        "Entity Identifier" as factset_entity_id
     FROM {{ source('ecoact_data', 'cdp_isin_facset') }})
SELECT * from cet
