/*
  Select the geogrphies from the sheet in EcoIntent description file
  The overlapping geographies are put in an array

  TODO: use or create a  cross-db macro instead of string_to_array
  https://github.com/dbt-labs/dbt-utils#cross-database-macros
*/
select
    "Shortname" as short_name,
    "ID" as id,
    "Name" as name,
    "Geography Classification" as classification,
    string_to_array(
        "Contained and Overlapping Geographies", ';'
    ) as contained_geographies
from {{ source("ref_data", "EI3.9_Geographies") }}
