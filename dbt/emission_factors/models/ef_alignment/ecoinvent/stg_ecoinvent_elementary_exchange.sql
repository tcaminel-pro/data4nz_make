/* 
Generated with 'sql_utils.db_columns_to_sql' function
*/
select
   "ID" as id,
   "Name" as name,
   "Compartment" as compartment,
   "Subcompartment" as subcompartment,
   "Unit Name" as unit_name,
   "CAS Number" as cas_number,
   "Comment" as comment,
   "Synonym" as synonym,
   "Formula" as formula 
from {{ source("ref_data", "EI3.9_Elementary_Exchanges")}}
