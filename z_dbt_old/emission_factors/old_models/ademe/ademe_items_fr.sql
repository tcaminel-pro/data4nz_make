/*
Select items from ADEME database
*/

with 
   refdata as (
      select
      *, 
      concat (nom_base_fr, 
         ' (', nom_attribut_fr, ')' ) as item_fr,
      json_strip_nulls(json_build_object (  
            'cde',code_de_la_categorie,
            'tgs', tags_fr, 
            'brd', nom_frontiere_fr,
            'dsc', commentaire_fr )) as description_fr
      from  {{ref("stg_base_carbone")}}  where type_ligne = 'Elément'),
    dataset as (
        select id as dataset_id from {{ref("datasets")}}
        where provider = 'Ademe' and version = '2022 - French'
        )
select
   dataset_id, 
   concat('ADM_', id) AS id,
   item_fr as item,
   unite_fr as product_unit,
   description_fr as description  /* todo : translate it */
 from refdata, dataset

/*
TODO: REPLACE CONCAT by dbt.concat, or by generic JSON formating stuff ? 
*/