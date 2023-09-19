

select * from {{ ref('q_and_a_rows_2022') }}
union 
select * from {{ ref('q_and_a_rows_2021') }}
union
select * from {{ ref('q_and_a_rows_2020') }}