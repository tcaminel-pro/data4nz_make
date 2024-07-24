/* Stage CEDA_2022_Lookup_US_USD
*/
select
    "CEDA Code" as id,
    'kg CO2e/$USD' as unit,
    "kg CO2e/$USD in 2021" as scope_123,
    "kg CO2e/$USD in 2021.1" as scope_1
from {{ source("ref_data", "CEDA_2022_Lookup_US_USD") }}
