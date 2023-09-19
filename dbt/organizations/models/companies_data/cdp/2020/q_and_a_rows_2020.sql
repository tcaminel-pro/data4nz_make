/*
Create a table with info from "extract_questions"
Column question is by default q_text, ie the question without its number

Copyright (C) 2023 Eviden. All rights reserved
*/
{{ config(
    materialized = 'table',
    indexes=[{'columns': ['organization']}, {'columns': ['sheet']} ]
)}}

select  year, 
        "Organization" as organization, 
        sheet, 
        row, 
        coalesce(q_text, q_section, question) as question, 
        value_str, 
        value_num
from {{ref("extract_questions_2020")}}
order by "Organization", sheet
