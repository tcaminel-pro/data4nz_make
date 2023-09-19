-- Mostly a non-regression test
-- We these values commes from the "test_ev_value.csv" and "test_ef_items" 
-- injected as seeds into the final tables, and the "ef_requests.csv" seed 
-- DOES NOT PASS if "ef_requests" has been updated 

select * from {{ ref("opdata_ef_row") }} 
where "ADPe" is null or "ADPe" != cast("ADPe" as integer) or area != 'Rest-of-World' or geo_rank != 4