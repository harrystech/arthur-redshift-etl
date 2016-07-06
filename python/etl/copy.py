#! /usr/bin/env python3

"""
Upload etc table design, SQL and (split) CSV files to S3 bucket.

This should be run after making changes to the table design files or the
SQL files describing CTAS or VIEWs in the etc repo.  Or after splitting CSV
files. The table design files are validated even during a dry-run.
"""

import concurrent.futures
import logging
import os.path

import etl
import etl.commands
import etl.config
import etl.load
import etl.pg
import etl.s3


def copy_source_to_s3(pool, source, tables, bucket_name, common_prefix, dry_run=False):
    for assoc_table_files in tables:
        source_table_name = assoc_table_files.source_table_name
        target_table_name = assoc_table_files.target_table_name
        design_file = open(assoc_table_files.design_file, 'r')
        table_design = etl.load.load_table_design(design_file, target_table_name)
        logging.debug("Validated table design for '%s'", table_design["name"])

        local_filename = assoc_table_files.design_file
        remote_directory = os.path.basename(os.path.dirname(local_filename))
        prefix = "{}/schemas/{}".format(common_prefix, remote_directory)
        pool.submit(etl.s3.upload_to_s3, local_filename, bucket_name, prefix, dry_run=dry_run)

        # XXX Add data and SQL

        # for file_type in ("Design", "SQL"):
        #     local_filename = files[file_type]
        #     if local_filename is not None:
        #         remote_directory = os.path.basename(os.path.dirname(local_filename))
        #         prefix = "{}/{}".format(prefix, remote_directory)
        #         pool.submit(etl.s3.upload_to_s3, local_filename, bucket_name, prefix, dry_run=args.dry_run)

        #if args.with_data and len(files["Data"]) > 0:
        #    for local_filename in files["Data"]:
        #        if not local_filename.endswith(".manifest"):
        #            remote_directory = os.path.basename(os.path.dirname(local_filename))
        #            prefix = "{}/{}".format(args.prefix, remote_directory)
        #            pool.submit(etl.s3.upload_to_s3, local_filename, bucket_name, prefix, dry_run=args.dry_run)
        #    # Manifest needs to be rewritten to reflect the latest bucket and prefix
        #    # XXX Switch to assoc table files
        #    manifest = etl.s3.write_manifest_file(files["Data"], bucket_name, prefix, dry_run=args.dry_run)
        #    executor.submit(etl.s3.upload_to_s3, manifest, bucket_name, prefix, dry_run=args.dry_run)


def copy_to_s3(args, settings):
    """
    Copy table design and SQL files from directory to S3 bucket.
    """
    bucket_name = settings("s3", "bucket_name")
    local_dirs = [args.table_design_dir, args.data_dir]
    selection = etl.TableNamePatterns.from_list(args.table)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    if args.git_modified:
        schemas_with_tables = etl.s3.find_modified_files(schemas, selection)
    else:
        schemas_with_tables = etl.s3.find_local_files(local_dirs, schemas, selection)

    if len(schemas_with_tables) == 0:
        logging.error("No applicable files found in %s", local_dirs)
    else:
        logging.info("Found files for %d schemas(s).", len(schemas_with_tables))
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for source in settings("sources"):
                tables = schemas_with_tables.get(source["name"])
                if tables:
                    copy_source_to_s3(executor, source, tables, bucket_name, args.prefix, dry_run=args.dry_run)
        if not args.dry_run:
            logging.info("Uploaded all files to 's3://%s/%s/'", bucket_name, args.prefix)
