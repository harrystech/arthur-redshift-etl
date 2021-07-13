-- Lookup actual table attributes for sorting columns and distribution styles.
-- This query needs admin privileges.
-- Args: selected_schemas
WITH table_info AS (
    SELECT "schema" AS schema_name
         , "table" AS table_name
         , encoded
         , diststyle AS distribution_style
         , sortkey1 AS first_sort_column
         , sortkey_num AS total_sort_columns
         , unsorted AS percentage_unsorted
    FROM pg_catalog.svv_table_info
)
SELECT schema_name
     , table_name
     , encoded
     , distribution_style
     , first_sort_column
     , total_sort_columns
     , percentage_unsorted
FROM table_info
WHERE schema_name IN %(selected_schemas)s
ORDER BY schema_name, table_name
