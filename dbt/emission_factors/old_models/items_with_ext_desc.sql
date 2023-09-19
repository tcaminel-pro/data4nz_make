/* Extend the desctiptions of items
  - add the list of areas where they have values
  - create and change 'desc' field with other information we have
    objective is the have better search. 

  Copyright (C) 2023 Eviden. All rights reserved
*/

with 
include_areas as (
    select dataset_id, item, product_unit, descr, cas, isic, proxies, 
    string_agg(distinct area, ', ') as areas
    from {{ref("ef_merged_items")}}  
    left join {{ref("ef_merged_values")}}    
        on ef_merged_items.id = ef_merged_values.id
    group by dataset_id, item, product_unit, descr, cas, isic, proxies)

select * from include_areas

/*
descr_extended as (
    select *,
    case 
        when descr is not null and isic is not null  
          then concat (item, ', described as: "', descr, 
            '". It is classified as: ',   , 
            '. Unit is: ',product_unit) 
        when descr is not null and isic is null  
          then concat (item, ', described as: "', descr, '". Unit is: ',product_unit) 
        when product_unit is not null or lower(product_unit) != 'unit' or lower(product_unit) != 'item'
          then concat (item, '. Unit is: ',product_unit) 
          -- todo: classify the units (length, mass, monetary, ...)
        else item
    end as descr_ext
    from include_areas),  
with_proxies as (
    select *, 
    case 
        when proxies is not null then 
            concat (descr_ext, '. Proxies are: ', proxies) 
        else descr_ext
        end as descr_ext_2
    from descr_extended) 
  */