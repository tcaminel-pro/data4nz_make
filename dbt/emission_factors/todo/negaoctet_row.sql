/* 
 Inspired by 
 https://github.com/dbt-labs/dbt-utils/blob/main/macros/sql/pivot.sql
 https://towardsdatascience.com/mastering-pivot-tables-in-dbt-832560a1a1c5 
 */
select
    id,
    {{
        dbt_utils.pivot(
            column="emission_factor",
            values=dbt_utils.get_column_values(
                table=ref("negaoctet_aligned"),
                column="emission_factor",
                where="emission_factor != '??'",
            ),
            then_value="value_ef",
        )
    }}
from {{ ref("negaoctet_aligned") }}
group by id
