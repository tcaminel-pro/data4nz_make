select distinct  
   product_name,
   activity_name 
from {{ ref("stg_ecoinvent_en15804_isic") }}
order by product_name, activity_name