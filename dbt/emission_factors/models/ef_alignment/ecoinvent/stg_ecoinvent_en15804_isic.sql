select
   "Activity UUID & Product UUID" as uuids,
   "Activity UUID" as a_id,
   "Activity Name" as activity_name,
   "Geography" as geography,
   "Time Period" as time_period,
   "Special Activity Type" as special_activity_type,
   "Sector" as sector,
   "ISIC Classification" as isic_classification,
   "ISIC Section" as isic_section,
   "Product UUID" as p_id,
   "Reference Product Name" as product_name,
   "CPC Classification" as cpc_classification,
   "Unit" as unit,
   "Product Information" as product_information,
   "CAS Number" as cas_number,
   "Cut-Off Classification" as cut_off_classification 
from {{ source("ref_data", "EI3.9_EN15804_AO") }}
