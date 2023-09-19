/*
   Create a table with
   - the sheet name
   - q_section: the questions adressed by the sheet  (usualy common part of the questions befire the ' -')
   - q_list: the list of questions in the sheet, without their number, and including "row_name"

	Copyright (C) 2023 Eviden. All rights reserved
*/
{{ config(materialized="table") }}
with
    rows_numbered as (select * from {{ ref("stg_rows_2021") }}),
    cte as (
        select distinct sq.sheet, q_section, question as q_text, col_num
        from rows_numbered
        left join
            {{ ref("sheet_question_2021") }} as sq
            on left(rows_numbered.sheet, 24) = left(sq.sheet_name, 24)
    )
select sheet, q_section, array_agg(q_text order by col_num) as q_list
from cte
where q_section is not null
group by sheet, q_section
