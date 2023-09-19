/* WITH 
    cte_ef       AS (SELECT * FROM {{ref('ecoinvent_ef_row')}}),
    cte_activity_name AS (SELECT * FROM {{ref('ecoinvent_aligned')}})
SELECT 
     item,
     product_unit, 
     countries,  
     description,
     cte_ef.*
FROM cte_activity_name 
JOIN cte_ef  ON cte_ef.id = cte_activity_name.id
*/
