/*
Create a table with info from "extract_questions"
Column question is by default q_text, ie the question without its number

Copyright (C) 2023 Eviden. All rights reserved
*/
{{ config(
    materialized = 'table',
    indexes=[{'columns': ['organization']}, {'columns': ['sheet']} ]
)}}

with 
question_list as (
    select  * from {{ref("sheet_question_2021")}} ),
qrows as (
    select * from {{ref("stg_rows_2021")}} )

select  year, 
        "Organization" as organization, 
        question_list.sheet, 
        row, 
        coalesce(question, q_section) as question, 
        value_str, 
        value_num
from qrows left join question_list 
    on left(qrows.sheet, 24) = left(question_list.sheet_name, 24)
order by "Organization", question_list.sheet asc

