SELECT schemaname
     , tablename
     , reverse(split_part(reverse(serialization_lib), '.', 1)) AS serialization_lib
     , serde_parameters
FROM svv_external_tables
