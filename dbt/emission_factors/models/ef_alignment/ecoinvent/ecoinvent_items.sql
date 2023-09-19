/* 
  Collect information related to EcoInvent items 
  and align with the shared scheme for GWP 
  Collected:
    - product description from 'Intermediate Exchanges' sheet in Ecoinvent overview
    - chemical proxies mapping from EcoAct
  Todo (?): sector, ...

  Copyright (C) 2023 Eviden. All rights reserved
*/
with
  /* Get data from the staged model (from the Excel file) */
  ei_stg as (
    select distinct 
      activity_name as item, 
      product_name,
      product_unit,
      product_amount,
      p_id,
      uuids as id
    from {{ ref("ecoinvent_rows_with_calculated") }}),
 
  /* Get product descriptions from another EcoInvent file and sheet */
  /* TOTO: find the information "within the dataset' they mention */
  descriptions as (
    select id, product_information, cas_number
    from {{ ref("stg_ecoinvent_intermediate_exchange") }}
    where product_information not like
       'Product information for this product is available within the datasets that %'  
       /* can be produce it, treat it, ...*/
    ),

  isic_class as (
    /* get ISIC classification */
    select uuids, isic_classification 
    from {{ ref("stg_ecoinvent_en15804_isic")}}
  ),
 
  /* Extend chemical information with substance that are close to the item in the database
     The mapping is maintanied by Lau (EcoAct) */ 
  proxy_mapping as (
    select ecoinvent_process_name,mapped_proxies
    from {{ ref("ecoinvent_chem_proxies") }}
  ),

  /* join the diffent data source we have */
  joined_data as (
    select
        ei_stg.*,
        product_information, 
        cas_number,
        isic_classification,
        mapped_proxies
    from ei_stg
      left join descriptions on ei_stg.p_id = descriptions.id
      left join proxy_mapping on ei_stg.item = proxy_mapping.ecoinvent_process_name
      left join isic_class  on ei_stg.id = isic_class.uuids ),

  /* get our id for that dataset */
  dataset as (
    select 'EI3.9' as dataset_id   -- TODO! make it more generic
    ),

  /* align to  the common schema, and put extra information as JSON fields */
  final as (
    select 
      dataset_id, 
      item,
      product_unit,
      id,
      json_strip_nulls(json_build_object ( 
        'cas', cas_number,
        'descr', product_information,
        'isic', isic_classification,
        'product_name', product_name,
        'product_amount', product_amount,
        'proxies', mapped_proxies)) as description
    from joined_data, dataset)

select * from final
order by item    /* just to ease test - could be removed */

