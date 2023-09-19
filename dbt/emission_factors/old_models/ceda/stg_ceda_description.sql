/* 
Stage CEDA_2022_Descriptions table.
Get one row per CEDA code by concatenating the  "Further Descriptions" field

see https://stackoverflow.com/questions/15847173/concatenate-multiple-result-rows-of-one-column-into-one-group-by-another-column

*/
select
    "CEDA Code" as id,
    "CEDA Description" as item,
    string_agg("Further Descriptions", '; ') as description
from {{ source("ref_data", "CEDA_2022_Descriptions") }}
group by 1, 2
