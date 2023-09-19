/*
Select items from ADEME database
*/

with 
   refdata as (
      select
      *, 
      concat (nom_base_en, 
            ' (', nom_attribut_en, ')' ) as item_en,
      concat (nom_base_fr, 
            ' (', nom_attribut_fr, ')' ) as item_fr,

      json_strip_nulls(json_build_object (  
                  'cde',code_de_la_categorie,
                  'tgs', tags_en, 
                  'brd', nom_frontiere_en,
                  'dsc', commentaire_en )) as description_en                 
      from  {{ref("stg_base_carbone")}}  where type_ligne = 'Elément'),

    dataset as (
        select id as dataset_id from {{ref("datasets")}}
        where provider = 'Ademe' and version = '2022 - English'
        ),
 
     final as (
      select
      dataset_id, 
      concat('ADM_', id) AS id,
      (case when nom_base_en is null 
            then CONCAT('[FR]', item_fr) else item_en end) as item,
      (case when unite_en is null 
            then unite_fr else unite_en end) as product_unit,
      description_en as description  /* todo : translate it */
      from refdata, dataset
     ) 
select * from final



/*
TODO: REPLACE CONCAT by dbt.concat, or by generic JSON formating stuff ? 
*/