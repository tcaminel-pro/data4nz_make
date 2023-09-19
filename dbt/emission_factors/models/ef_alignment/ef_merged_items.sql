/* flatten the description JSON field into several columns.
 */

{{ config(materialized="table") }}


SELECT  dataset_id, item, product_unit, id,
        product_name, product_amount, descr,cas, isic, proxies   
FROM {{ref("ef_merged_items_view")}}, json_to_record( description) as 
     x(descr text, cas text, isic text, proxies text, product_name text, product_amount int )
order by dataset_id, item