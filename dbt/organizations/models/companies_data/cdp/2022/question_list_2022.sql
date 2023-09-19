/*
   Create a table with
   - the sheet name
   - q_section: the questions adressed by the sheet  (usualy common part of the questions befire the ' -')
   - q_list: the list of questions in the sheet, without their number, and including "row_name"

	Copyright (C) 2023 Eviden. All rights reserved
*/

-- toto: take same cet structure than in 2020
{{ config(materialized="table") }}
with cte as (
	select distinct sq.sheet , sq.q_section, q_num, eq.q_text 
    from {{ref("sheet_question_2022")}} as sq
	join {{ref("extract_questions_2022")}}  as eq 
	on sq.sheet = eq.sheet 
	order by eq.q_num 
	) 
select sheet, q_section, array_agg(q_text order by q_num ) as q_list
from cte
where q_section is not null
group by sheet,q_section