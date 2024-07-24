

SELECT replace("Question Number", '/', '_') as sheet, 
       replace(replace("Question Number", '/', ''), '-','') as sheet_name, 
       "Question Name" as q_section
FROM {{source("cdp_excel_files","QuestionsList_values_2021")}}

