/* 
Generated with 'sql_utils.db_columns_to_sql' function
*/
select
    "ID" as id,
    "Name" as name,
    "Unit Name" as unit_name,
    "CAS Number" as cas_number,
    "Comment" as comment,
    "By-product Classification" as by_product_classification,
    "CPC Classification" as cpc_classification,
    "Product Information" as product_information,
    "Synonym" as synonym
from {{ source("ref_data", "EI3.9_Intermediate_Exchanges") }}
