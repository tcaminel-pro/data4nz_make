/*
    Copyright (C) 2023 Eviden. All rights reserved
*/

{{ config(materialized="table") }}
select  year, account_name, score from
{{ref("stg_rating_2022")}}
where score is not null