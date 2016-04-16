from contextlib import closing
import logging

import etl
import etl.load
import etl.pg
import etl.s3


def update_table_or_view(args, settings, select_ctas_or_view, cmd_sequence):
    assert select_ctas_or_view in ("CTAS", "VIEW")

    dw = etl.env_value(settings("data_warehouse", "etl_access"))
    table_owner = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    bucket_name = settings("s3", "bucket_name")

    schemas = [source["name"] for source in settings("sources")]
    if "view" in args:
        selection = etl.TableNamePattern(args.view)
    else:
        selection = etl.TableNamePattern(args.table)
    files_in_s3 = etl.s3.find_files(bucket_name, args.prefix, schemas, selection)
    tables_with_queries = [(table_name, table_files["Design"], table_files["SQL"])
                           for table_name, table_files in files_in_s3
                           if table_files["SQL"] is not None]
    if len(tables_with_queries) == 0:
        logging.getLogger(__name__).error("No applicable files found in 's3://%s/%s'", bucket_name, args.prefix)
    else:
        with closing(etl.pg.connection(dw)) as conn:
            for table_name, design_file, sql_file in tables_with_queries:
                with closing(etl.s3.get_file_content(bucket_name, design_file)) as content:
                    table_design = etl.load.load_table_design(content, table_name)
                # Pick either CTAS or VIEW based on parameter:
                if table_design["source_name"] != select_ctas_or_view:
                    continue
                with closing(etl.s3.get_file_content(bucket_name, sql_file)) as content:
                    query = content.read().decode()
                with conn:
                    cmd_sequence(conn, table_design, table_name, table_owner, etl_group, user_group, query, args)
