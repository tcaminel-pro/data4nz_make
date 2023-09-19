/*
Add  columns and an index for full text search in PostgreSQL

See : https://www.postgresql.org/docs/current/textsearch-tables.html

@todo:  make indexed fields and weights configurable
*/
{% macro add_text_index(table) %}
{% set sql %}
    ALTER TABLE {{ table }} DROP COLUMN IF EXISTS text_index_col CASCADE;
    DROP INDEX IF EXISTS text_search_idx;
    ALTER TABLE {{ table }} ADD COLUMN text_index_col TSVECTOR GENERATED ALWAYS AS 
        (setweight(to_tsvector('english', coalesce("item", '')), 'A') 
        || ' ' || 
        setweight(to_tsvector('english', coalesce("description"::text, '')), 'C')) 
        STORED;
    CREATE INDEX text_search_idx ON {{ table }} USING GIN (text_index_col);
{% endset %}

{% do run_query(sql) %}
{% do log("Text index created on " ~ table, info=True) %}
{% endmacro %}
