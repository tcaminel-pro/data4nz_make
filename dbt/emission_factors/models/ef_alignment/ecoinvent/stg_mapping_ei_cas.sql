/* 
Generated with 'sql_utils.db_columns_to_sql' function and slightly modified
*/

select
   "SUBSTANCE: CAS Number" as substance_cas_number,
   "SUBSTANCE: Substance Description" as substance_description,
   "Lookup function (helper)" as lookup_function_helper,
   "ecoinvent process name" as ecoinvent_process_name,
   "Country/region" as country_region,
   "Country/region no." as country_region_no,
   "Material " as material,
   "Material no." as material_no,
   "Concat_name" as concat_name 
from {{source("ref_data", "Mapping_EI_CAS")}} 