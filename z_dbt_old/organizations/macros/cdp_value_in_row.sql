{# 
    Find a value in a CDP  sheet where the answer is a table.
    We need to make a self join : first find the right row, then retrieve the value.
    
    return  colums: year, organization, value_str, value_num, 
    Args:
    - sheet : name of the Excel sheet 
    - question_name
    - row_name : value to take in the column 'row_name' of the sheet
    - value_name : either 'value_num' (for numerical values), or 'value_str'
#}


{% macro cdp_value_in_row(sheet, question_name, row_name, value_name = 'value_num') %}

with find_row as (
    select  organization, row
    from {{ref('stg_q_and_a_rows')}} 
    where sheet = 'C6.1' and question = 'row_name' and value_str = '{{row_name}}'),
find_value as (
    select stg_q_and_a_rows.organization, year, {{value_name}}
    from {{ref('stg_q_and_a_rows')}} 
    join find_row on stg_q_and_a_rows.organization = find_row.organization 
       and stg_q_and_a_rows.row = find_row.row
    where question = '{{question_name}}'
)

{%- set other_val = 'value_str' if value_name == 'value_num' else 'value_str'  -%}

select *, null as {{other_val}}  from find_value
{% endmacro %}