/*
   Create a table with
   - the sheet name
   - q_section: the questions adressed by the sheet  (usualy common part of the questions befire the ' -')
   - q_list: the list of questions in the sheet, without their number, and including "row_name" (or NULL)

	Copyright (C) 2023 Eviden. All rights reserved
*/
{{ config(materialized="table") }}
with 
sheet_question as (
	-- get the question associated to the sheet
	select distinct  sheet, q_section 
  	from  {{ref("extract_questions_2020")}} ),
questions_in_columns as (
	-- for each sheet, get all (sub) questions in the sheet columns 
	select distinct sq.sheet , sq.q_section, q_num, eq.q_text 
    from sheet_question as sq
	join {{ref("extract_questions_2020")}}  as eq 
	on sq.sheet = eq.sheet 
	order by eq.q_num ) ,
aggregated_questions_in_columns as (
	-- concatenate the (sub) questions in the columns
	select sheet, q_section, array_agg(q_text order by q_num ) as q_list
	from questions_in_columns
	where q_section is not null
	group by sheet,q_section)

select * from aggregated_questions_in_columns