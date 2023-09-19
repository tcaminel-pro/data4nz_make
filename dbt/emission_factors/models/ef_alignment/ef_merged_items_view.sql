
/*  Create a table from different emission factors datasets, with their description
    The text fields are indexed for full text search
*/
with
    cte as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ecoinvent_items"),
                    ref("negaoctet_items"),
                    ref("carbonmind_items"),
                    ref("test_ef_items"),
                ],
                source_column_name="None",
            )
        }}
    )
select * from cte
