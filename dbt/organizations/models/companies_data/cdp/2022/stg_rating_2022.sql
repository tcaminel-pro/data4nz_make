/*
   Data scrapped from CDP site. Just rename fields
   
   Copyright (C) 2023 Eviden. All rights reserved
*/
select
    cast("search_results__project_year" as int) as year,
    "search_results__account_name" as account_name,
    "search_results__account_name href" as account_name_href,
    "investor-program__score_band_single" as score
from {{ source("cdp_scrapped_files", "cdp_rating_2022") }}
