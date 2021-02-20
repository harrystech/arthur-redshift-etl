-- List all tables in external schemas and mark which ones are part of the configuration.
-- Args: selected_schemas
SELECT schemaname AS schema_name
     , tablename AS table_name
     , schemaname IN %(selected_schemas)s AS is_configured
     , reverse(split_part(reverse(serialization_lib), '.', 1)) AS serialization_lib
     , serde_parameters
FROM svv_external_tables
