

select distinct  
   activity_name,
   isic_classification,
   special_activity_type,
   product_information,
   isic_section   
from {{ ref("stg_ecoinvent_en15804_isic") }}