"""
Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.

Table designs are dictionaries of dictionaries or lists etc.
"""

from contextlib import closing
import logging

import jsonschema
import simplejson as json
import yaml
import yaml.parser

import etl
import etl.config
from etl.errors import TableDesignParseError, TableDesignSemanticError, TableDesignSyntaxError
import etl.pg
import etl.file_sets
import etl.s3


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file).
    The table design is validated before being returned.
    """
    try:
        table_design = yaml.safe_load(stream)
    except yaml.parser.ParserError as exc:
        raise TableDesignParseError() from exc
    return validate_table_design(table_design, table_name)


def load_table_design_from_localfile(local_filename, table_name):
    """
    Load (and validate) table design file in local file system.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Loading local table design from '%s'", local_filename)
    try:
        with open(local_filename) as f:
            table_design = load_table_design(f, table_name)
    except:
        logger.error("Failed to load table design from '%s'", local_filename)
        raise
    return table_design


def load_table_design_from_s3(bucket_name, design_file, table_name):
    """
    Download (and validate) table design from file in S3.
    """
    with closing(etl.s3.get_s3_object_content(bucket_name, design_file)) as content:
        table_design = load_table_design(content, table_name)
    return table_design


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not right.

    Phase 1 of validation is based on a schema and json-schema validation.
    Phase 2 is built on specific rules that I couldn't figure out how
    to run inside json-schema.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Trying to validate table design for '%s'", table_name.identifier)
    validate_table_design_syntax(table_design, table_name)
    validate_table_design_semantics(table_design, table_name)
    return table_design


def validate_table_design_syntax(table_design, table_name):
    """
    Validate table design based on the (JSON) schema (which can only check syntax but not values).
    Raise an exception if anything is amiss.
    """
    logger = logging.getLogger(__name__)
    validation_internal_errors = (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
        json.scanner.JSONDecodeError)
    # Two things can break here: reading the schema, which is validated, and then reading the table design.
    try:
        table_design_schema = etl.config.load_json("table_design.schema")
    except validation_internal_errors:
        logger.critical("Internal Error: Schema in 'table_design.schema' is not valid")
        raise
    try:
        jsonschema.validate(table_design, table_design_schema)
    except validation_internal_errors as exc:
        raise TableDesignSyntaxError("Failed to validate table design for '%s'" % table_name.identifier) from exc


def validate_semantics_of_view(table_design):
    """
    Check for semantics that only apply to views.

    Basically, definitions of views may only contain column names.
    """
    # This error occurs when you change from CTAS to VIEW and then forget to remove the extra information
    # for the columns, like type, sql_type.
    for column in table_design["columns"]:
        if len(column) != 1:
            raise TableDesignSemanticError("Too much information for column of a VIEW: %s" % list(column))


def validate_semantics_of_table(table_design):
    """
    Check for semantics that apply to tables only ... either upstream sources or CTAS.
    """
    for column in table_design["columns"]:
        if column.get("identity", False) and not column.get("not_null", False):
            # NULL columns may not be primary key (identity)
            raise TableDesignSemanticError("identity column must be set to not null")

    identity_columns = [column["name"] for column in table_design["columns"] if column.get("identity", False)]
    if len(identity_columns) > 1:
        raise TableDesignSemanticError("only one column should have identity")

    surrogate_keys = table_design.get("constraints", {}).get("surrogate_key", [])
    if len(surrogate_keys) and not surrogate_keys == identity_columns:
        raise TableDesignSemanticError("surrogate key must be identity")

    # Make sure that whenever we reference a column that that column is actually part of the table's columns
    column_set = frozenset(column["name"] for column in table_design["columns"] if not column.get("skipped"))
    column_list_references = [
        ('constraints', 'primary_key'),
        ('constraints', 'natural_key'),
        ('constraints', 'surrogate_key'),
        ('constraints', 'foreign_key'),
        ('constraints', 'unique'),
        ('attributes', 'interleaved_sort'),
        ('attributes', 'compound_sort')
    ]
    invalid_col_list_template = "{obj}'s {key} list contains unknown columns"
    for obj, key in column_list_references:
        if not column_list_has_columns(column_set, table_design.get(obj, {}).get(key)):
            raise TableDesignSemanticError(invalid_col_list_template.format(obj=obj, key=key))

    if table_design["source_name"] != "CTAS" and "depends_on" in table_design:
        raise TableDesignSemanticError("upstream table has dependencies")


def validate_table_design_semantics(table_design, table_name):
    """
    Validate table design against rule set based on values (e.g. name of columns).
    Raise an exception if anything is amiss.
    """
    if table_design["name"] != table_name.identifier:
        raise TableDesignSemanticError("Name of table (%s) must match target (%s)" % (table_design["name"],
                                                                                      table_name.identifier))

    if table_design["source_name"] == "VIEW":
        validate_semantics_of_view(table_design)
    else:
        validate_semantics_of_table(table_design)


def column_list_has_columns(valid_columns, candidate_columns):
    """
    Accepts a set of known columns and a list of strings that may be columns (or a string that should be a column)
    Returns True if the possible column list is found within column_set and False otherwise

    >>> column_list_has_columns({'a'}, 'a')
    True
    >>> column_list_has_columns({'fish'}, 'a')
    False
    >>> column_list_has_columns({'a', 'b'}, ['b', 'a'])
    True
    >>> column_list_has_columns({'a', 'b'}, ['b', 'c'])
    False
    """
    if not candidate_columns:
        return True
    if not isinstance(candidate_columns, list):
        candidate_columns = [candidate_columns]
    for column in candidate_columns:
        if column not in valid_columns:
            return False
    return True
