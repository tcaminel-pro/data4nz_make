select
   "internalUUID" as uuid,
   "flowName" as flow_name,
   "type" as type,
   "CAS" as cas,
   "processName" as process_name,
   "country" as country,
   "ISOTwoLetterCountryCode" as country_code,
   "processDescription" as process_description,
   "OverallQualityShort" as overall_quality,
   "bioCarbonContent" as bio_carbon_content,
   "carbonContent" as carbon_content,
   "EF v3.0 EN15804 - climate change - global warming potential (GW" as global_gwp,
   "EF v3.0 EN15804 - climate change: biogenic - global warming pot" as biogenic_gwp,
   "EF v3.0 EN15804 - climate change: fossil - global warming poten" as fossil_gwp,
   "EF v3.0 EN15804 - climate change: land use and land use change " as land_use_gwp 
from {{ source("ref_data", "Carbon-minds_2022_EF3.0") }} 