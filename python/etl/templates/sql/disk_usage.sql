-- Collect disk usage information. Combine multiple tables when they belong to the same target.
-- This query needs admin privileges.
-- Args: selected_schemas
WITH etl_table_info AS (
    SELECT REPLACE(REPLACE("schema", 'etl_backup$$', ''), 'etl_staging$$', '') AS schema_name
         , "table" AS table_name
         , CASE
               WHEN "schema" LIKE 'etl_backup$$%%' THEN 'backup'
               WHEN "schema" LIKE 'etl_staging$$%%' THEN 'staging'
               ELSE 'normal'
           END AS table_position
         , size
         , pct_used
         , diststyle AS diststyle
         , skew_rows AS skew_rows
    FROM pg_catalog.svv_table_info
)
SELECT schema_name
     , table_name
     , BOOL_OR(table_position = 'backup') AS has_backup
     , BOOL_OR(table_position = 'staging') AS has_staging
     , SUM(size) AS total_storage_mb
     , MAX(CASE table_position WHEN 'normal' THEN pct_used END) AS pct_used
     , MAX(CASE table_position WHEN 'normal' THEN diststyle END) AS diststyle
     , MAX(CASE table_position WHEN 'normal' THEN skew_rows END) AS skew_rows
FROM etl_table_info
WHERE schema_name IN %(selected_schemas)s
GROUP BY schema_name, table_name
ORDER BY schema_name, table_name
