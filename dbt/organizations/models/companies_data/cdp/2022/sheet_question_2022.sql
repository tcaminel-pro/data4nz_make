-- return the question associated to a sheet
-- 	Copyright (C) 2023 Eviden. All rights reserved
select distinct  sheet, q_section 
  from  {{ref("extract_questions_2022")}}
