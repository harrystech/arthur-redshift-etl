"""
The purpose of "validate" is to exercise the configuration, schemas, and queries that make up
the data warehouse description (and eventually, its state).

This includes static and runtime checks.  As always, the "dry-run" option tries to run as much
as possible but doesn't change any state (i.e. doesn't write or create tables or...)

Validating the warehouse schemas (definition of upstream tables and their transformations) includes:
* Syntax checks (valid json?)
* "Schema" checks (valid design files, "schema" here refers to the description of valid fields etc.)
* Semantic checks (meaningful design files -- checks that are not described by a "schema")
* Reload-ability (finds source schemas corresponding to unloaded tables)
* Valid SQL in CTAS or views
* Correct description of dependencies
"""

from contextlib import closing
import difflib
import logging
from typing import List

import psycopg2
import simplejson as json

from etl.errors import CyclicDependencyError, ReloadConsistencyError, TableDesignError
import etl.pg
import etl.relation
from etl.relation import RelationDescription


def validate_design_file_semantics(descriptions, keep_going=False):
    """
    Load local design files and validate them along the way against schemas and semantics.

    Return list for successfully validated descriptions if you want to keep going.
    Or raise exception on validation error.
    """
    logger = logging.getLogger(__name__)
    ok = []
    for description in descriptions:
        try:
            logger.info("Loading and validating file '%s'", description.design_file_name)
            if description.table_design:
                ok.append(description)
        except TableDesignError:
            if not keep_going:
                raise
    return ok


def _check_dependencies(observed, table_design):
    """
    Compare actual dependencies to a table design object and return instructions for logger

    >>> _check_dependencies(['abc.123', '123.abc'], dict(name='fish', depends_on=['123.abc']))  # doctest: +ELLIPSIS
    'Dependency tracking mismatch! payload =...]}'

    >>> _check_dependencies(['abc.123', '123.abc'], dict(name='fish', depends_on=['123.abc', 'abc.123']))

    """
    expected = table_design.get('depends_on', [])

    observed_deps = set(observed)
    expected_deps = set(expected)
    if not observed_deps == expected_deps:
        return 'Dependency tracking mismatch! payload = {}'.format(json.dumps(dict(
                 full_dependencies=observed,
                 table=table_design['name'],
                 actual_but_unlisted=list(observed_deps - expected_deps),
                 listed_but_not_actual=list(expected_deps - observed_deps))))


def _check_columns(observed, table_design):
    """
    Compare actual columns in query to those in table design object and return instructions for logger

    >>> _check_columns(['a', 'b'], dict(columns=[dict(name='b'), dict(name='a')])) # doctest: +ELLIPSIS
    'Column listing mismatch! Diff of observed vs expected follows:...'

    >>> _check_columns(['a', 'b'], dict(columns=[dict(name='a'), dict(name='b')]))

    """
    observed = list(observed)
    expected = [column["name"] for column in table_design["columns"] if not column.get('skipped', False)]

    # handle identity columns by inserting a column into observed in the expected position
    for index, column in enumerate(table_design['columns']):
        if column.get('identity', False):
            observed.insert(index, column['name'])

    if observed != expected:
        return 'Column listing mismatch! Diff of observed vs expected follows: {}'.format(
                 '\n'.join(difflib.context_diff(observed, expected)))


def validate_single_transform(conn, description, keep_going=False):
    """
    Test-run a relation (CTAS or VIEW) by creating a temporary view.

    With a view created, we can extract dependency information and a list of columns
    to make sure table design and query match up.
    """
    logger = logging.getLogger(__name__)
    # FIXME Switch to using a temporary view (starts with '#' and has no schema)
    tmp_view_name = etl.TableName(schema=description.target_table_name.schema,
                                  table='$'.join(["arthur_temp", description.target_table_name.table]))
    ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(tmp_view_name, description.query_stmt)
    logger.info("Creating view '%s' for table '%s'" % (tmp_view_name.identifier, description.identifier))
    etl.pg.execute(conn, ddl_stmt)

    # based off example query in AWS docs; *_p is for parent, *_c is for child
    dependency_stmt = """
        SELECT DISTINCT
               n_c.nspname AS dependency_schema
             , c_c.relname AS dependency_name
          FROM pg_class c_p
          JOIN pg_depend d_p ON c_p.relfilenode = d_p.refobjid
          JOIN pg_depend d_c ON d_p.objid = d_c.objid
          -- the following OR statement covers the case where a COPY has issued a new OID for an upstream table
          JOIN pg_class c_c ON d_c.refobjid = c_c.relfilenode OR d_c.refobjid = c_c.oid
          LEFT JOIN pg_namespace n_p ON c_p.relnamespace = n_p.oid
          LEFT JOIN pg_namespace n_c ON c_c.relnamespace = n_c.oid
         WHERE c_p.relname = '{table}' AND n_p.nspname = '{schema}'
            -- do not include the table itself in its dependency list
           AND c_p.oid != c_c.oid
        """.format(schema=tmp_view_name.schema, table=tmp_view_name.table)

    dependencies = [etl.TableName(schema=row['dependency_schema'], table=row['dependency_name']).identifier
                    for row in etl.pg.query(conn, dependency_stmt)]
    dependencies.sort()
    logger.info("Dependencies discovered: [{}]".format(', '.join(dependencies)))

    comparison_output = _check_dependencies(dependencies, description.table_design)
    if comparison_output:
        if keep_going:
            logger.warning(comparison_output)
        else:
            raise TableDesignError(comparison_output)
    else:
        logger.info('Dependencies listing in design file matches SQL')

    columns_stmt = """
        SELECT a.attname
          FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n
         WHERE c.relname = '{0.table}'
           AND a.attnum > 0
           AND a.attrelid = c.oid
           AND a.atttypid = t.oid
           AND c.relnamespace = n.oid
           AND n.nspname = '{0.schema}'
         ORDER BY attnum ASC
        """.format(tmp_view_name)

    actual_columns = [row['attname'] for row in etl.pg.query(conn, columns_stmt)]
    comparison_output = _check_columns(actual_columns, description.table_design)
    if comparison_output:
        if keep_going:
            logger.warning(comparison_output)
        else:
            raise TableDesignError(comparison_output)
    else:
        logger.info('Column listing in design file matches column listing in SQL')

    logger.info("Dropping view '%s'", tmp_view_name.identifier)
    etl.pg.execute(conn, "DROP VIEW IF EXISTS {}".format(tmp_view_name))


def validate_transforms(dsn: dict, descriptions: List[RelationDescription], keep_going: bool=False) -> None:
    """
    Validate transforms (CTAS or VIEW relations) by trying to run them in the database.
    This allows us to check their syntax, their dependencies, etc.
    """
    logger = logging.getLogger(__name__)
    transforms = [description for description in descriptions
                  if description.is_ctas_relation or description.is_view_relation]
    if not transforms:
        logger.info("No transforms found, skipping their validation")
        return

    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in transforms:
            try:
                with etl.pg.log_error():
                    validate_single_transform(conn, description, keep_going=keep_going)
            except (etl.ETLError, psycopg2.Error):
                if keep_going:
                    logger.exception("Ignoring failure to validate '%s' and proceeding as requested:",
                                     description.identifier)
                else:
                    raise


def get_list_difference(list1, list2):
    """
    Return list of elements that help turn list1 into list2 -- a "minimal" list of
    differences based on changing one list into the other.

    >>> sorted(get_list_difference(["a", "b"], ["b", "a"]))
    ['b']
    >>> sorted(get_list_difference(["a", "b"], ["a", "c", "b"]))
    ['c']
    >>> sorted(get_list_difference(["a", "b", "c", "d", "e"], ["a", "c", "b", "e"]))
    ['c', 'd']

    The last list happens because the matcher asks to insert 'c', then delete 'c' and 'd' later.
    """
    diff = set()
    matcher = difflib.SequenceMatcher(None, list1, list2)  # None means skip junk detection
    for code, left1, right1, left2, right2 in matcher.get_opcodes():
        if code != 'equal':
            diff.update(list1[left1:right1])
            diff.update(list2[left2:right2])
    return diff


def validate_reload(descriptions: List[RelationDescription], keep_going: bool):
    """
    Verify that columns between unloaded tables and reloaded tables are the same.

    Once the designs are validated, we can unload a relation 's.t' with a target 'u' and
    then extract and load it back into 'u.t'.

    Note that the order matters for these lists of columns.  (Which is also why we
    can't take the (symmetric) difference between columns but must be careful checking
    the column lists.)
    """
    logger = logging.getLogger(__name__)
    unloaded_descriptions = [d for d in descriptions if d.is_unloadable]
    descriptions_lookup = {d.identifier: d for d in descriptions}

    for unloaded in unloaded_descriptions:
        logger.debug("Checking whether '%s' is loaded back in", unloaded.identifier)
        reloaded = etl.TableName(unloaded.unload_target, unloaded.target_table_name.table)
        if reloaded.identifier in descriptions_lookup:
            description = descriptions_lookup[reloaded.identifier]
            logger.info("Checking for consistency between '%s' and '%s'", unloaded.identifier, description.identifier)
            unloaded_columns = unloaded.unquoted_columns
            reloaded_columns = description.unquoted_columns
            if unloaded_columns != reloaded_columns:
                diff = get_list_difference(reloaded_columns, unloaded_columns)
                logger.error("Column difference detected between '%s' and '%s'",
                             unloaded.identifier, description.identifier)
                logger.error("You need to replace, insert and/or delete in '%s': %s",
                             description.identifier, etl.join_with_quotes(diff))
                if not keep_going:
                    raise ReloadConsistencyError("Unloaded relation '%s' failed to match counterpart"
                                                 % unloaded.identifier)


def validate_designs(dsn: dict, descriptions: List[RelationDescription], keep_going=False, skip_deps=False) -> None:
    """
    Make sure that all table design files pass the validation checks.
    """
    logger = logging.getLogger(__name__)

    valid_descriptions = validate_design_file_semantics(descriptions, keep_going=keep_going)
    validate_reload(valid_descriptions, keep_going=keep_going)
    try:
        ordered_descriptions = etl.relation.order_by_dependencies(valid_descriptions)
    except CyclicDependencyError:
        if keep_going:
            logger.exception("Failed to determine evaluation order, proceeding as requested")
            ordered_descriptions = valid_descriptions
        else:
            raise

    if skip_deps:
        logger.info("Skipping validation of transforms against database")
    else:
        validate_transforms(dsn, ordered_descriptions, keep_going=keep_going)
