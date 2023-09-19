/* extract info from the stg_rows (version 2022)
    say the column/question is:
        "C6.5_C1_Account for your organization’s gross bla bla. - Evaluation status"
    then:
    - 'q_num' is the 'C1' in the  question"
    - 'q_section' is "Account for your organization’s gross bla bla."
    - 'q_text' is "Evaluation status"
    If the question is like "C1.1_Is there board-level oversight of bla bla?", 
    then  q_text = q_section  = "Is there board-level oversight of bla bla?"
    and q_num = "C1.1"
    'row_name' is a special case - its q_num is 0  (to be sorted first)

    Copyright (C) 2023 Eviden. All rights reserved
*/
select
    stg_rows_2022.*,
    coalesce((regexp_match(question, '(C.*)_.*'))[1], '0') as q_num,
    coalesce(
        (regexp_match(question, '(C.*)_(.*[\.?]) -'))[2],
        (regexp_match(question, '(C.*)_(.*)'))[2]
    ) as q_section,
    coalesce(
        (regexp_match(question, ' - (.*)'))[1],
        (regexp_match(question, '(C.*)_(.*)'))[2]
    ) as q_text
from {{ ref("stg_rows_2022") }}
