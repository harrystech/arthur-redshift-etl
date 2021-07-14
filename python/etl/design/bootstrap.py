import logging
import os.path
import re
from contextlib import closing
from typing import Dict, Iterable, List, Optional

import psycopg2.errors
import simplejson as json
from psycopg2.extensions import connection as Connection  # only for type annotation

import etl.config
import etl.db
import etl.design.load
import etl.file_sets
import etl.relation
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.design import Attribute, ColumnDefinition, TableDesign, diff_table_designs
from etl.errors import ETLSystemError, TableDesignValidationError
from etl.names import TableName, TableSelector, TempTableName, join_with_single_quotes
from etl.relation import RelationDescription

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def fetch_tables(cx: Connection, source: DataWarehouseSchema, selector: TableSelector) -> List[TableName]:
    """
    Retrieve tables (matching selector) for this source, return as a list of TableName instances.

    The :source configuration contains an "allowlist" (which tables to include) and a
    "denylist" (which tables to exclude). Note that "exclude" always overrides "include."
    The list of tables matching the allowlist but not the denylist can be further narrowed
    down by the pattern in :selector.
    """
    # Look for relations ('r', ordinary tables), materialized views ('m'), and views ('v').
    result = etl.db.query(
        cx,
        """
        SELECT nsp.nspname AS "schema"
             , cls.relname AS "table"
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid
         WHERE cls.relname NOT LIKE 'tmp%%'
           AND cls.relname NOT LIKE 'pg_%%'
           AND cls.relkind IN ('r', 'm', 'v')
         ORDER BY nsp.nspname
                , cls.relname
         """,
    )
    found = []
    for row in result:
        source_table_name = TableName(row["schema"], row["table"])
        target_table_name = TableName(source.name, row["table"])
        for reject_pattern in source.exclude_tables:
            if source_table_name.match_pattern(reject_pattern):
                logger.debug("Table '%s' matches denylist", source_table_name.identifier)
                break
        else:
            for accept_pattern in source.include_tables:
                if source_table_name.match_pattern(accept_pattern):
                    if selector.match(target_table_name):
                        found.append(source_table_name)
                        logger.debug("Table '%s' is included in result set", source_table_name.identifier)
                        break
                    else:
                        logger.debug(
                            "Table '%s' matches allowlist but is not selected", source_table_name.identifier
                        )
    logger.info(
        "Found %d table(s) matching patterns; allowlist=%s, denylist=%s, subset='%s'",
        len(found),
        source.include_tables,
        source.exclude_tables,
        selector,
    )
    return found


def fetch_attributes(cx: Connection, table_name: TableName) -> List[Attribute]:
    """Retrieve attribute definition (column names and types)."""
    # Make sure to turn on "User Parameters" in the Database settings of PyCharm so that `%s`
    # works in the editor.
    if isinstance(table_name, TempTableName) and table_name.is_late_binding_view:
        stmt = """
            SELECT col_name AS "name"
                 , col_type AS "sql_type"
                 , FALSE AS "not_null"
              FROM pg_get_late_binding_view_cols() cols(
                       view_schema name
                     , view_name name
                     , col_name name
                     , col_type varchar
                     , col_num int)
             WHERE view_schema LIKE %s
               AND view_name = %s
             ORDER BY col_num
            """
    else:
        stmt = """
            SELECT a.attname AS "name"
                 , pg_catalog.format_type(t.oid, a.atttypmod) AS "sql_type"
                 , a.attnotnull AS "not_null"
              FROM pg_catalog.pg_attribute AS a
              JOIN pg_catalog.pg_class AS cls ON a.attrelid = cls.oid
              JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
              JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
             WHERE a.attnum > 0  -- skip system columns
               AND NOT a.attisdropped
               AND ns.nspname LIKE %s
               AND cls.relname = %s
             ORDER BY a.attnum
            """
    attributes = etl.db.query(cx, stmt, (table_name.schema, table_name.table))
    return [Attribute(**att) for att in attributes]


def fetch_constraints(cx: Connection, table_name: TableName) -> List[Dict[str, List[str]]]:
    """
    Retrieve table constraints from database by looking up indices.

    We will only check primary key constraints and unique constraints. If constraints have
    predicates (like a WHERE clause) or use functions (like COALESCE(column, value), we'll skip
    them.

    (To recreate the constraint, we could use `pg_get_indexdef`.)
    """
    # We need to make two trips to the database because Redshift doesn't support functions
    # on int2vector types. So we find out which indices exist (with unique constraints) and how
    # many attributes are related to each, then we look up the attribute names with our
    # exquisitely hand-rolled "where" clause. See
    # http://www.postgresql.org/message-id/10279.1124395722@sss.pgh.pa.us for further explanations.
    stmt_index = """
        SELECT i.indexrelid AS index_id
             , ic.relname AS index_name
             , CASE
                   WHEN i.indisprimary THEN 'primary_key'
                   ELSE 'unique'
               END AS "constraint_type"
             , i.indnatts AS nr_atts
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_index AS i ON cls.oid = i.indrelid
          JOIN pg_catalog.pg_class AS ic ON i.indexrelid = ic.oid
         WHERE i.indisunique
           AND i.indpred IS NULL
           AND i.indexprs IS NULL
           AND ns.nspname LIKE %s
           AND cls.relname = %s
         ORDER BY "constraint_type", ic.relname"""
    indices = etl.db.query(cx, stmt_index, (table_name.schema, table_name.table))

    stmt_att = """
        SELECT a.attname AS "name"
          FROM pg_catalog.pg_attribute AS a
          JOIN pg_catalog.pg_index AS i ON a.attrelid = i.indrelid
          WHERE i.indexrelid = %s
            AND ({cond})
          ORDER BY a.attname"""
    found = []
    for index_id, index_name, constraint_type, nr_atts in indices:
        cond = " OR ".join("a.attnum = i.indkey[%d]" % i for i in range(nr_atts))
        attributes = etl.db.query(cx, stmt_att.format(cond=cond), (index_id,))
        if attributes:
            columns = [att["name"] for att in attributes]
            constraint: Dict[str, List[str]] = {constraint_type: columns}
            logger.info(
                "Index '%s' of '%s' adds constraint %s",
                index_name,
                table_name.identifier,
                json.dumps(constraint, sort_keys=True),
            )
            found.append(constraint)
    return found


def fetch_dependencies(cx: Connection, table_name: TableName) -> List[str]:
    """
    Lookup dependencies (other relations).

    Note that this will return an empty list for a late-binding view.
    """
    # See https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_constraint_dependency.sql  # noqa: E501
    stmt = """
        SELECT DISTINCT
               target_ns.nspname AS "schema"
             , target_cls.relname AS "table"
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_depend AS dep ON cls.oid = dep.refobjid
          JOIN pg_catalog.pg_depend AS target_dep ON dep.objid = target_dep.objid
          JOIN pg_catalog.pg_class AS target_cls ON target_dep.refobjid = target_cls.oid
                                                AND cls.oid <> target_cls.oid
          JOIN pg_catalog.pg_namespace AS target_ns ON target_cls.relnamespace = target_ns.oid
         WHERE ns.nspname LIKE %s
           AND cls.relname = %s
         ORDER BY "schema", "table"
        """
    dependencies = etl.db.query(cx, stmt, (table_name.schema, table_name.table))
    return [TableName(**row).identifier for row in dependencies]


def search_query_step(line: str) -> Optional[Dict[str, str]]:
    """
    Return any S3-based tables found in the query step (part of a query plan).

    >>> plan = '-> S3 Nested Subquery ex_schema.table location:"s3://bucket/..."'
    >>> search_query_step(plan)
    {'s3_table': 'ex_schema.table'}
    >>> plan = '-> S3 Seq Scan ex_schema.table location:'
    >>> search_query_step(plan)
    {'s3_table': 'ex_schema.table'}
    >>> plan = '-> S3 Seq Scan ex_schema.table alias location:'
    >>> search_query_step(plan)
    {'s3_table': 'ex_schema.table'}
    >>> plan = '-> XN Seq Scan on table'
    >>> search_query_step(plan)
    {'xn_table': 'table'}
    """
    ws_re = re.compile(r"\s+")
    # Note that in the regex below, whitespace outside '[ ]' or '\ ' is ignored.
    scan_re = re.compile(
        r"""->[ ]
            S3\ Nested\ Subquery\ (?P<s3_table_nested>\w+\.\w+)\ location:
            | S3\ Seq\ Scan\ (?P<s3_table_seq>\w+\.\w+)(?:\ \w+)?\ location:
            | XN\ Seq\ Scan\ on\ (?P<xn_table>\w+)
        """,
        re.VERBOSE,
    )
    match = scan_re.search(ws_re.sub(" ", line))
    if not match:
        return None
    values = match.groupdict()
    if values["s3_table_nested"]:
        return {"s3_table": values["s3_table_nested"]}
    if values["s3_table_seq"]:
        return {"s3_table": values["s3_table_seq"]}
    if values["xn_table"]:
        return {"xn_table": values["xn_table"]}
    return None


def fetch_dependency_hints(cx: Connection, stmt: str) -> Optional[List[str]]:
    """Parse a query plan for hints of which relations we might depend on."""
    s3_dependencies = []
    xn_dependencies = []

    logger.debug("Looking at query plan to find dependencies")
    try:
        plan = etl.db.explain(cx, stmt)
    except psycopg2.errors.InvalidSchemaName as exc:
        logger.warning("Cannot fetch dependencies: %s", str(exc).strip())
        return None

    for line in plan:
        maybe = search_query_step(line)
        if maybe is None:
            continue
        if "s3_table" in maybe:
            s3_dependencies.append(maybe["s3_table"])
        if "xn_table" in maybe:
            xn_dependencies.append(maybe["xn_table"])

    if s3_dependencies and xn_dependencies:
        # Unfortunately the tables aren't qualified with a schema so we can't use the info.
        logger.warning(
            "Found dependencies in S3 AND these not in S3: %s",
            join_with_single_quotes(xn_dependencies),
        )
    return sorted(s3_dependencies)


def create_partial_table_design(conn: Connection, source_table_name: TableName, target_table_name: TableName):
    """
    Start a table design for the relation.

    This returns a partial table design that contains
        - the name (identifier of our target table)
        - full column information (extracted from database source or data warehouse)
        - a description (with a timestamp)

    What is missing then to make it a valid table design is at least the "source_name".
    """
    type_maps = etl.config.get_dw_config().type_maps
    as_is_attribute_type = type_maps["as_is_att_type"]  # source tables and CTAS
    cast_needed_attribute_type = type_maps["cast_needed_att_type"]  # only source tables
    default_attribute_type = type_maps["default_att_type"]  # default (fallback)

    source_attributes = fetch_attributes(conn, source_table_name)
    if not source_attributes:
        raise RuntimeError("failed to find attributes (check your query)")
    unique_source_attributes = frozenset(attribute.name for attribute in source_attributes)
    if len(source_attributes) > len(unique_source_attributes):
        raise RuntimeError(f"found {len(source_attributes)} attributes but there are duplicates")
    target_columns = [
        ColumnDefinition.from_attribute(
            attribute, as_is_attribute_type, cast_needed_attribute_type, default_attribute_type
        )
        for attribute in source_attributes
    ]
    table_design = {
        "name": target_table_name.identifier,
        "description": "",
        "columns": [column.to_dict() for column in target_columns],
    }
    return table_design


def create_table_design_for_source(
    conn: Connection,
    source_table_name: TableName,
    target_table_name: TableName,
    existing_relation: Optional[RelationDescription],
) -> dict:
    """
    Create table design for a table in an upstream source.

    We gather the constraints from the source. (Note that only upstream tables can have constraints
    derived from inspecting the database.)

    If there is an existing table design, it is used to update the one bootstrapped from the source.
    """
    table_design = create_partial_table_design(conn, source_table_name, target_table_name)
    table_design["source_name"] = f"{target_table_name.schema}.{source_table_name.identifier}"
    constraints = fetch_constraints(conn, source_table_name)
    if constraints:
        table_design["constraints"] = constraints
    if existing_relation is None:
        return table_design

    existing_table_design = existing_relation.table_design
    if "description" in existing_table_design and not existing_table_design["description"].startswith(
        ("Automatically generated on", "Automatically updated on")
    ):
        table_design["description"] = existing_table_design["description"].strip()

    existing_columns = {column["name"]: column for column in existing_table_design["columns"]}
    new_columns = []
    for column in table_design["columns"]:
        column_name = column["name"]
        if column_name not in existing_columns:
            new_columns.append(column)
            continue
        existing_column = existing_columns[column_name]
        if existing_column.get("skipped", False):
            new_columns.append(existing_column)
            continue
        for key in ("description", "encoding"):
            if key in existing_column:
                column[key] = existing_column[key]

        # When the expression and SQL type have been adjusted, keep that adjustment
        # but only for cases where the "default" kicked in.
        # TODO(tom): Is there a safer way to keep these modifications?
        if (
            column["type"] == "string"
            and column.get("source_sql_type") == existing_column.get("source_sql_type")
            and column["sql_type"] != existing_column["sql_type"]
        ):
            logger.warning(
                "Keeping previous SQL type and expression for '%s.%s'", source_table_name, column_name
            )
            for key in ("sql_type", "expression"):
                if key in existing_column:
                    column[key] = existing_column[key]

        new_columns.append(column)
    table_design["columns"] = new_columns

    if "extract_settings" in existing_table_design:
        table_design["extract_settings"] = existing_table_design["extract_settings"]

    if "attributes" in existing_table_design:
        # TODO(tom): Should check columns in the attributes here
        table_design["attributes"] = existing_table_design["attributes"]

    if "constraints" not in table_design:
        return table_design

    skipped_columns = {column["name"] for column in table_design["columns"] if column.get("skipped")}
    new_constraints = []
    for constraint in table_design["constraints"]:
        [[_, constraint_columns]] = constraint.items()  # unpacking list of single dict
        if any(column in skipped_columns for column in constraint_columns):
            logger.info("Dropping constraint using skipped columns: %s", constraint)
            continue
        new_constraints.append(constraint)
    table_design["constraints"] = new_constraints
    return table_design


def create_partial_table_design_for_transformation(
    conn: Connection,
    tmp_view_name: TempTableName,
    relation: RelationDescription,
    update_keys: Optional[Iterable[str]] = None,
) -> dict:
    """
    Create a partial design that's applicable to transformations.

    This partial design:
        - cleans up the column information (dropping accidental expressions)
        - adds dependencies (which only transformations can have)
        - and optionally updates from the existing table design
    """
    table_design = create_partial_table_design(conn, tmp_view_name, relation.target_table_name)
    # When working with CTAS or VIEW, the type casting doesn't make sense but sometimes sneaks in.
    for column in table_design["columns"]:
        if "expression" in column:
            del column["expression"]
        if "source_sql_type" in column:
            del column["source_sql_type"]

    if tmp_view_name.is_late_binding_view:
        dependencies = fetch_dependency_hints(conn, relation.query_stmt)
    else:
        dependencies = fetch_dependencies(conn, tmp_view_name)
    if dependencies:
        table_design["depends_on"] = dependencies

    if update_keys is None or relation.design_file_name is None:
        return table_design

    logger.info("Update of existing table design file for '%s' in progress", relation.identifier)
    existing_table_design = relation.table_design
    selected_update_keys = frozenset(update_keys)

    if (
        "description" in selected_update_keys
        and "description" in existing_table_design
        and not existing_table_design["description"].startswith(
            ("Automatically generated on", "Automatically updated on")
        )
    ):
        table_design["description"] = existing_table_design["description"].strip()

    if "columns" in selected_update_keys:
        # If the old design had added an identity column, we carry it forward
        # here (and always as the first column).
        identity = [column for column in existing_table_design["columns"] if column.get("identity")]
        if identity:
            table_design["columns"][:0] = identity
            table_design["columns"][0]["encoding"] = "raw"

        existing_column = {column["name"]: column for column in existing_table_design["columns"]}
        for column in table_design["columns"]:
            if column["name"] in existing_column:
                # This modifies in-place until I have more time to fix this.
                update_column_definition(relation.identifier, column, existing_column[column["name"]])

    # Now do the rest of the update keys which require less attention to details.
    remaining_keys = selected_update_keys.difference(("columns", "description")).intersection(
        existing_table_design
    )
    for copy_key in sorted(remaining_keys):
        # TODO(tom): May have to cleanup columns in constraints and attributes!
        table_design[copy_key] = existing_table_design[copy_key]

    return table_design


def update_column_definition(target_table: str, new_column: dict, old_column: dict) -> dict:
    """
    Update (in place) the bootstrapped column definition with existing information.

    This carefully merges the information from inspecting the view created for the transformation
    with information from the existing table design.

    Copied directly: "description", "encoding", "references", and "not_null"
    Updated carefully: "sql_type" and "type" (always together)

    >>> update_column_definition("s.t", {
    ...     "name": "doctest", "sql_type": "integer", "type": "int"
    ... }, {
    ...     "name": "doctest", "sql_type": "bigint", "type": "long"
    ... })
    {'name': 'doctest', 'sql_type': 'bigint', 'type': 'long'}
    >>> update_column_definition("s.t", {
    ...     "name": "doctest", "sql_type": "numeric(18,4)", "type": "string"
    ... }, {
    ...     "name": "doctest", "sql_type": "DECIMAL(12, 2)", "type": "string"
    ... })
    {'name': 'doctest', 'sql_type': 'numeric(12,2)', 'type': 'string'}
    >>> update_column_definition("s.t", {
    ...     "name": "doctest", "sql_type": "character varying(100)", "type": "string"
    ... }, {
    ...     "name": "doctest", "sql_type": "Varchar(200)", "type": "string"
    ... })
    {'name': 'doctest', 'sql_type': 'character varying(200)', 'type': 'string'}
    """
    column_name = new_column["name"]
    assert old_column["name"] == column_name

    ok_to_copy = frozenset({"description", "encoding", "references", "not_null"})
    have_old_value = ok_to_copy.intersection(old_column)
    new_column.update({key: value for key, value in old_column.items() if key in have_old_value})

    # When we update from VIEW to CTAS, there's not much to copy from.
    if "sql_type" not in old_column:
        return new_column

    new_sql_type = new_column["sql_type"]
    old_sql_type = old_column["sql_type"].strip()
    logger.debug("Trying to update column '%s': new='%s', old='%s'", column_name, new_sql_type, old_sql_type)

    # Upgrade to "larger" type if that was selected previously.
    if new_sql_type in ("integer", "bigint"):
        if old_sql_type == "bigint" and new_sql_type == "integer":
            new_column.update(sql_type="bigint", type="long")
            logger.warning(
                "Keeping previous definition for column '%s.%s': '%s' (consider a cast)",
                target_table,
                column_name,
                new_column["sql_type"],
            )
        return new_column

    numeric_re = re.compile(r"(?:numeric|decimal)\(\s*(?P<precision>\d+),\s*(?P<scale>\d+)\)", re.IGNORECASE)
    new_numeric = numeric_re.search(new_sql_type)
    if new_numeric:
        old_numeric = numeric_re.search(old_sql_type)
        if old_numeric and new_numeric.groups() != old_numeric.groups():
            new_column["sql_type"] = "numeric({precision},{scale})".format_map(old_numeric.groupdict())
            # TODO(tom): Be smarter about precision and scale separately.
            # TODO(tom): Allow to check for a fixed number of (precision, scale) combinations.
            logger.warning(
                "Keeping previous definition for column '%s.%s': '%s'",
                target_table,
                column_name,
                new_column["sql_type"],
            )
        return new_column

    varchar_re = re.compile(r"(?:varchar|character varying)\((?P<size>\d+)\)", re.IGNORECASE)
    new_text = varchar_re.search(new_sql_type)
    if new_text:
        old_text = varchar_re.search(old_sql_type)
        if old_text:
            new_size = int(new_text.groupdict()["size"])
            old_size = int(old_text.groupdict()["size"])
            if new_size < old_size:
                logger.debug(
                    "Found for '%s.%s': '%s', used previously: '%s'",
                    target_table,
                    column_name,
                    new_column["sql_type"],
                    old_column["sql_type"],
                )
                new_column["sql_type"] = f"character varying({old_size})"
                logger.warning(
                    "Keeping previous definition for column '%s.%s': '%s' (please add a cast)",
                    target_table,
                    column_name,
                    new_column["sql_type"],
                )
        return new_column

    # Those types above are all that we understand how to update.
    return new_column


def create_table_design_for_ctas(
    conn: Connection, tmp_view_name: TempTableName, relation: RelationDescription, update: bool
):
    """
    Create new table design for a CTAS.

    If tables are referenced, they are added in the dependencies list.
    If :update is True, try to merge additional information from any existing table design file.
    """
    update_keys = ["description", "unload_target", "columns", "constraints", "attributes"] if update else None
    table_design = create_partial_table_design_for_transformation(conn, tmp_view_name, relation, update_keys)
    table_design["source_name"] = "CTAS"
    return table_design


def create_table_design_for_view(
    conn: Connection, tmp_view_name: TempTableName, relation: RelationDescription, update: bool
):
    """
    Create (and return) new table design suited for a view.

    Views are expected to always depend on some other relations. (Also, we only keep the names
    for columns.)

    If :update is True, try to merge additional information from any existing table design file.
    """
    update_keys = ["description", "unload_target", "columns"] if update else None
    # This creates a full column description with types (testing the type-maps), then drops all of
    # that except for names and descriptions.
    table_design = create_partial_table_design_for_transformation(conn, tmp_view_name, relation, update_keys)
    table_design["columns"] = [
        {key: value for key, value in column.items() if key in ("name", "description")}
        for column in table_design["columns"]
    ]
    table_design["source_name"] = "VIEW"
    return table_design


def create_table_design_for_transformation(
    conn: Connection, kind: str, relation: RelationDescription, update=False
) -> dict:
    # Use a quick check of the query plan whether we depend on external schemas.
    if kind not in ("CTAS", "VIEW"):
        raise ETLSystemError(f"unexpected source name: {kind}")

    with relation.matching_temporary_view(conn, as_late_binding_view=True) as tmp_view_name:
        dependencies = fetch_dependency_hints(conn, relation.query_stmt)
        if dependencies is None:
            raise RuntimeError("failed to query for dependencies")
        if any(dependencies):
            logger.info(
                "Looks like %s has external dependencies, proceeding with caution", relation.identifier
            )
            if kind == "VIEW":
                raise RuntimeError("VIEW not supported for transformations that depend on extrenal tables")
            return create_table_design_for_ctas(conn, tmp_view_name, relation, update)

    with relation.matching_temporary_view(conn, as_late_binding_view=False) as tmp_view_name:
        if kind == "VIEW":
            return create_table_design_for_view(conn, tmp_view_name, relation, update)
        return create_table_design_for_ctas(conn, tmp_view_name, relation, update)


def save_table_design(
    target_table_name: TableName,
    table_design: dict,
    filename: str,
    overwrite=False,
    dry_run=False,
) -> None:
    """
    Write new table design file to disk.

    Although these files are generated by computers, they get read by humans. So we try to be nice
    here and write them out with a specific order of the keys, like have name and description
    towards the top.
    """
    # Validate before writing to make sure we don't drift between bootstrap and JSON schema.
    etl.design.load.validate_table_design(table_design, target_table_name)
    this_table = target_table_name.identifier
    if dry_run:
        logger.info("Dry-run: Skipping writing table design file for '%s' to '%s'", this_table, filename)
        return

    if os.path.exists(filename) and not overwrite:
        logger.warning(
            "Skipping writing new table design for '%s' since '%s' already exists", this_table, filename
        )
        return

    logger.info("Writing new table design file for '%s' to '%s'", this_table, filename)
    with open(filename, "w") as o:
        o.write(TableDesign.as_string(table_design))
    logger.debug("Completed writing '%s'", filename)


def normalize_and_create(directory: str, dry_run=False) -> str:
    """
    Make sure the directory exists and return normalized path to it.

    This will create all intermediate directories as needed.
    """
    name = os.path.normpath(directory)
    if not os.path.exists(name):
        if dry_run:
            logger.debug("Dry-run: Skipping creation of directory '%s'", name)
        else:
            logger.info("Creating directory '%s'", name)
            os.makedirs(name)
    return name


def create_table_designs_from_source(
    source, selector, local_dir, relations, update=False, replace=False, dry_run=False
):
    """
    Create table design files for tables from a single upstream source to local directory.

    Whenever some table designs already exist locally, validate them, update them or replace
    them against the information found from the upstream source.
    """
    source_dir = os.path.join(local_dir, source.name)
    normalize_and_create(source_dir, dry_run=dry_run)
    relation_lookup = {relation.source_table_name: relation for relation in relations}

    try:
        logger.info("Connecting to database source '%s' to look for tables", source.name)
        with closing(etl.db.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            source_tables = fetch_tables(conn, source, selector)
            for source_table_name in source_tables:
                if source_table_name in relation_lookup and not (update or replace):
                    logger.info(
                        "Skipping '%s' from source '%s' because table design already exists: '%s'",
                        source_table_name.identifier,
                        source.name,
                        relation_lookup[source_table_name].design_file_name,
                    )
                    continue
                relation = relation_lookup.get(source_table_name) if update else None
                target_table_name = TableName(source.name, source_table_name.table)
                table_design = create_table_design_for_source(
                    conn, source_table_name, target_table_name, relation
                )
                if relation is not None and relation.table_design == table_design:
                    logger.info(
                        f"No updates detected in table design for {target_table_name:x}, skipping write"
                    )
                    continue
                filename = os.path.join(
                    source_dir, f"{source_table_name.schema}-{source_table_name.table}.yaml"
                )
                save_table_design(
                    target_table_name, table_design, filename, overwrite=update or replace, dry_run=dry_run
                )

        logger.info("Done with %d table(s) from source '%s'", len(source_tables), source.name)
    except Exception:
        logger.error("Error while processing source '%s'", source.name)
        raise

    existent = frozenset(relation.source_table_name.identifier for relation in relations)
    upstream = frozenset(name.identifier for name in source_tables)
    not_found = upstream.difference(existent)
    if not_found:
        logger.warning(
            "New table(s) in source '%s' without local design: %s",
            source.name,
            join_with_single_quotes(not_found),
        )
    too_many = existent.difference(upstream)
    if too_many:
        logger.error(
            "Local table design(s) without table in source '%s': %s",
            source.name,
            join_with_single_quotes(too_many),
        )

    return len(source_tables)


def bootstrap_sources(selector, table_design_dir, local_files, update=False, replace=False, dry_run=False):
    """
    Download schemas from database tables and write or update local design files.

    This will create new design files locally if they don't already exist for any relations tied
    to upstream database sources. If one already exists locally, the file is update or replaced or
    kept as-is depending on the "update" and "replace" arguments.

    This is a callback of a command.
    """
    dw_config = etl.config.get_dw_config()
    selected_database_schemas = [
        schema
        for schema in dw_config.schemas
        if schema.is_database_source and selector.match_schema(schema.name)
    ]
    database_schema_names = {schema.name for schema in selected_database_schemas}
    existing_relations = [
        RelationDescription(file_set)
        for file_set in local_files
        if file_set.source_name in database_schema_names and file_set.design_file_name
    ]
    if existing_relations:
        RelationDescription.load_in_parallel(existing_relations)

    total = 0
    for schema in selected_database_schemas:
        relations = [relation for relation in existing_relations if relation.source_name == schema.name]
        total += create_table_designs_from_source(
            schema, selector, table_design_dir, relations, update, replace, dry_run=dry_run
        )
    if not total:
        logger.warning("Found no matching tables in any upstream source for '%s'", selector)


def bootstrap_transformations(
    local_dir, local_files, source_name, check_only=False, update=False, replace=False, dry_run=False
):
    """
    Download design information for transformations by test-running in the data warehouse.

    "source_name" should be "CTAS" or "VIEW or None (in which case the relation type currently
    specified will continue to be used).

    This is a callback of a command.
    """
    dw_config = etl.config.get_dw_config()
    transformation_schema = {schema.name for schema in dw_config.schemas if schema.has_transformations}
    transforms = [file_set for file_set in local_files if file_set.source_name in transformation_schema]
    if not (check_only or replace or update):
        # Filter down to new transformations: SQL files without matching YAML file
        transforms = [file_set for file_set in transforms if not file_set.design_file_name]
    if not transforms:
        logger.warning("Found no new queries without matching design files")
        return

    relations = [RelationDescription(file_set) for file_set in transforms]
    if check_only or update or (replace and source_name is None):
        logger.info("Loading existing table design file(s)")
        try:
            RelationDescription.load_in_parallel(relations)
        except Exception:
            logger.warning("Make sure that table design files exist and are valid before trying to update")
            raise

    check_only_errors = 0
    with closing(etl.db.connection(dw_config.dsn_etl, autocommit=True)) as conn:
        for index, relation in enumerate(relations):
            logger.info(
                "Working on transformation '%s' (%d/%d)", relation.identifier, index + 1, len(relations)
            )
            # Be careful to not trigger a load of an unknown design file by accessing "source_name".
            actual_kind = source_name or (relation.source_type if relation.design_file_name else None)
            try:
                table_design = create_table_design_for_transformation(
                    conn, actual_kind, relation, update or check_only
                )
            except RuntimeError as exc:
                if check_only:
                    print(f"Failed to create table design for {relation:x}: {exc}")
                    check_only_errors += 1
                    continue
                else:
                    raise

            if check_only:
                if relation.table_design != table_design:
                    check_only_errors += 1
                    print(f"Change detected in table design for {relation:x}")
                    print(
                        diff_table_designs(
                            relation.table_design, table_design, relation.design_file_name, "bootstrap"
                        )
                    )
                continue

            if update and relation.table_design == table_design:
                logger.info(f"No updates detected in table design for {relation:x}, skipping write")
                continue

            source_dir = os.path.join(local_dir, relation.source_name)
            # Derive preferred name from the current design or SQL file.
            if relation.design_file_name is not None:
                filename = relation.design_file_name
            elif relation.sql_file_name is not None:
                filename = re.sub(r".sql$", ".yaml", relation.sql_file_name)
            else:
                filename = os.path.join(
                    source_dir,
                    f"{relation.target_table_name.schema}-{relation.target_table_name.table}.yaml",
                )
            save_table_design(
                relation.target_table_name,
                table_design,
                filename,
                overwrite=update or replace,
                dry_run=dry_run,
            )

    if check_only_errors:
        raise TableDesignValidationError(f"found {check_only_errors} table design(s) that would be rewritten")
    if check_only:
        print("Congratulations. There were no changes in table design files.")
