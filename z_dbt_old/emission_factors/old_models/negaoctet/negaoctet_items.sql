/*
  Select items from NegaOcted LCA dataset, and align to the shared schema
*/
select distinct
    'NO22' as dataset_id, 
    manuf_bilan as id, 
    name as item, 
    product_unit, 
    null as description
from {{ ref("stg_negaoctet") }} as negaoctet
