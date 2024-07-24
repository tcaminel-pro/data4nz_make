/*
  Select EF  values from ADEME database and align to shared schema

  TODAY only GWP
  TODO :  Unpivot  'stg_base_carbone' and compute (TBD EcoAct)
   (co2f + ch4f) AS "GWPf"
   ch4b AS "GWPb ",
   n2o AS "N2O",
   co2b AS "CO2b"

   */
with 
    cte as (
        select *,
        case 
            when localisation = 'France continentale' then 'France'
            when localisation = 'Monde' then 'Global'
            when localisation = 'Europe' then 'Europe' 
            when localisation = 'Autre pays du monde' then 
                coalesce (sous_localisation_en, sous_localisation_fr, 'GLO')
            /* TODO: get ISO code */
            else concat ('region:', sous_localisation_fr)
            end as area       
        from {{ ref("stg_base_carbone") }}
        where type_ligne = 'Elément'
    )

 
select
    concat('ADM_', id) as id,
    area,
    'GWP' as emission_factor, 
    area as countries,    /* TODO: extend to 'countries' */
    total_poste_non_decompose as value_ef,
    json_build_object ( 
        'geo',concat (localisation, '-',sous_localisation_fr)) as meta

from cte
