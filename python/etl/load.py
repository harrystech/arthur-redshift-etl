import configparser
import io
import logging
import os.path

import jsonschema
import psycopg2
import yaml

from etl import TableName
import etl.config
import etl.dump
import etl.s3
import etl.pg


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file). The table design is
    validated before returned.
    """
    table_design = yaml.safe_load(stream)
    validate_table_design(table_design, table_name)
    return table_design


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not right.
    """
    table_design_schema = etl.config.load_json("table_design.schema")
    jsonschema.validate(table_design, table_design_schema)
    if table_design["name"] != table_name.identifier:
        raise ValueError("Name of table (%s) must match target (%s)" % (table_design["name"], table_name.identifier))


def format_column_list(columns):
    """
    Return string with comma-separated, delimited column names
    """
    return ", ".join('"{}"'.format(column) for column in columns)


def assemble_table_ddl(table_design):
    """
    Columns must have a name and a SQL type (compatible with Redshift).
    They may have an attribute of the compression encoding and the nullable
    constraint.
    Other column attributes and constraints should be resolved as table
    attributes (e.g. distkey) and table constraints (e.g. primary key).
    Tables may have attributes such as a distribution style and sort key.
    Depending on the distribution style, they may also have a distribution key.
    Supported table constraints include primary key (most likely "id"),
    unique constraint, and foreign keys.
    """
    table_name = TableName(*table_design["name"].split('.', 1))
    columns = table_design["fields"]
    constraints = table_design.get("table_constraints", {})
    attributes = table_design.get("table_attributes", {})
    # Pick up name and SQL type.  Column attribute may be 'encode' and constraint may be 'not_null'
    s_columns = []
    for column in columns:
        if 'encode' in column:
            if column.get('not_null', False):
                s_column = '"{name}" {sql_type} ENCODE {encode} NOT NULL'
            else:
                s_column = '"{name}" {sql_type} ENCODE {encode}'
        else:
            if column.get('not_null', False):
                s_column = '"{name}" {sql_type} NOT NULL'
            else:
                s_column = '"{name}" {sql_type}'
        s_columns.append(s_column.format(**column))
    # Pick up any table constraints
    # TODO Check whether more than one unique or foreign constraint is supported
    s_constraints = []
    if "primary_key" in constraints:
        s_constraints.append('PRIMARY KEY ( {} )'.format(format_column_list(constraints["primary_key"])))
    if "unique" in constraints:
        s_constraints.append('UNIQUE ( {} )'.format(format_column_list(constraints["unique"])))
    if "foreign_key" in constraints:
        # Either column_name, ref_table, ref_column or column_name, ref_table (but that is turned into the first form)
        reference = constraints["foreign key"]
        if len(reference) == 2:
            reference = reference + reference[:1]
        s_constraints.append('FOREIGN KEY ( {0} ) REFERENCES {1} ( {2} )'.format(reference))
    s_attributes = []
    if "diststyle" in attributes:
        s_attributes.append('DISTSTYLE {}'.format(attributes["diststyle"]))
    if "distkey" in attributes:
        s_attributes.append('DISTKEY ( "{}" )'.format(attributes["distkey"]))
    if "sortkey" in attributes:
        s_attributes.append('SORTKEY ( {} )'.format(format_column_list(attributes["sortkey"])))
    return "CREATE TABLE IF NOT EXISTS {} ({},\n   {})\n   {}".format(table_name,
                                                                      " ,\n   ".join(s_columns),
                                                                      " \n   ".join(s_constraints),
                                                                      " \n   ".join(s_attributes))


def create_table(conn, table_name, bucket, ddl_file, drop_table=False, dry_run=False):
    """
    Run the CREATE TABLE statement before trying to copy data into table.

    """
    logger = logging.getLogger(__name__)
    content = etl.s3.get_file_content(bucket, ddl_file)
    table_design = load_table_design(io.StringIO(content), table_name)
    ddl_stmt = assemble_table_ddl(table_design)

    if dry_run:
        logger.info("Dry-run: Skipping creation of %s from 's3://%s/%s'", table_name.identifier, bucket.name, ddl_file)
        logger.debug("Skipped:\n%s", ddl_stmt)
    else:
        if drop_table:
            logger.debug("Dropping table (if exists) %s", table_name.identifier)
            etl.pg.execute(conn, "DROP TABLE IF EXISTS {}".format(table_name), debug=True)
            logger.info("Creating new table %s", table_name.identifier)
        else:
            logger.info("Creating table (if exists) %s", table_name.identifier)
        etl.pg.execute(conn, ddl_stmt, debug=True)
        if not drop_table:
            logger.info("Truncating table %s", table_name.identifier)
            etl.pg.execute(conn, "TRUNCATE {}".format(table_name), debug=True)


def read_aws_credentials(from_file="~/.aws/credentials"):
    """
    Read access key and secret key from file (by default, ~/.aws/credentials)
    or from environment variables, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    """
    if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
        access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        logging.getLogger(__name__).info("Read credentials from environment")
    else:
        parser = configparser.ConfigParser()
        found = parser.read(os.path.expanduser(from_file))
        if len(found) != 1:
            raise RuntimeError("Unable to read your '%s' file" % from_file)
        access_key_id = parser.get('default', 'aws_access_key_id')
        secret_access_key = parser.get('default', 'aws_secret_access_key')
        logging.getLogger(__name__).info("Read credentials for COPY command from '%s'", found[0])
    return {'access_key_id': access_key_id, 'secret_access_key': secret_access_key}


def copy_data(conn, table_name, bucket, csv_file, credentials, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command
    """
    access = "aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}".format(**credentials)
    filename = "s3://{}/{}".format(bucket.name, csv_file)
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping copy for {} from '{}'".format(table_name.identifier, filename))
    else:
        logger.info("Copying data into {} from '{}'".format(table_name.identifier, filename))
        try:
            etl.pg.execute(conn, """COPY {} FROM %s
                                    CREDENTIALS %s
                                    FORMAT AS CSV GZIP IGNOREHEADER {}
                                    NULL AS '\\\\N'
                                    TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
                                    TRUNCATECOLUMNS
                                 """.format(table_name, etl.dump.N_HEADER_LINES), (filename, access), debug=True)
        except psycopg2.Error as exc:
            if "stl_load_errors" in exc.pgerror:
                info = etl.pg.query(conn, """SELECT query, starttime, filename, colname, type, col_length,
                                                    line_number, position, err_code, err_reason
                                               FROM stl_load_errors
                                              WHERE session = pg_backend_pid()
                                              ORDER BY starttime DESC
                                              LIMIT 1""")
                values = '\n  '.join(["{}: {}".format(k, row[k]) for row in info for k in row.keys()])
                logger.info("Information from stl_load_errors:\n  %s", values)
            raise


def assemble_ctas_ddl(table_design):
    """
    Return statement to create table based on a query, something like:
    CREATE TABLE table_name ( column_name [, ... ] ) table_attributes AS query
    """
    # TODO Can this be refactored with assemble_table_ddl?
    table_name = TableName(*table_design["name"].split('.', 1))
    columns = table_design["fields"]
    attributes = table_design.get("table_attributes", {})
    s_columns = []
    for column in columns:
        s_columns.append('"{name}"'.format(**column))
    s_attributes = []
    if "diststyle" in attributes:
        s_attributes.append('DISTSTYLE {}'.format(attributes["diststyle"]))
    if "distkey" in attributes:
        s_attributes.append('DISTKEY ( "{}" )'.format(attributes["distkey"]))
    if "sortkey" in attributes:
        s_attributes.append('SORTKEY ( {} )'.format(format_column_list(attributes["sortkey"])))
    return "CREATE TABLE {} ({}) {}\n   AS\n".format(table_name, " ,\n   ".join(s_columns), " \n   ".join(s_attributes))


def create_table_as(conn, table_name, bucket, design_file, sql_file, dry_run=False):
    """
    Run the CREATE TABLE AS statement to load data.
    """
    content = etl.s3.get_file_content(bucket, design_file)
    table_design = load_table_design(io.StringIO(content), table_name)
    query_stmt = etl.s3.get_file_content(bucket, sql_file)
    ddl_stmt = assemble_ctas_ddl(table_design) + query_stmt
    if dry_run:
        logging.info("Dry-run: Skipping creation of %s", table_name.identifier)
    else:
        logging.info("Creating table %s using query", table_name.identifier)
        etl.pg.execute(conn, "DROP TABLE IF EXISTS {}".format(table_name), debug=True)
        etl.pg.execute(conn, ddl_stmt, debug=True)


def grant_access(conn, table_name, user_group, dry_run=False):
    """
    Grant select permission to users on new/updated tables.
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping permissions grant on %s", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Granting select access on %s to %s", table_name.identifier, user_group)
        etl.pg.grant_select(conn, table_name.schema, table_name.table, user_group, debug=True)


def vacuum_analyze(conn, table_name, dry_run=False):
    """
    Final step ... tidy up the warehouse before guests come over
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping vacuum-analyze %s", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Running vacuum-analyze step on %s", table_name.identifier)
        etl.pg.execute(conn, "VACUUM {}".format(table_name), debug=True)
        etl.pg.execute(conn, "ANALYZE {}".format(table_name), debug=True)
