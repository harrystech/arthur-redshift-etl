"""
Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.

Table designs are dictionaries of dictionaries or lists etc.
"""

import logging
from contextlib import closing

import funcy as fy
import yaml
import yaml.parser

import etl
import etl.config
import etl.db
import etl.file_sets
import etl.s3
from etl.errors import SchemaValidationError, TableDesignParseError, TableDesignSemanticError, TableDesignSyntaxError
from etl.text import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file).
    The table design is validated before being returned.
    """
    try:
        table_design = yaml.safe_load(stream)
    except yaml.parser.ParserError as exc:
        raise TableDesignParseError(exc) from exc

    etl.config.validate_with_schema(table_design, "table_design.schema")

    # We used to specify constraints using an object (before v0.24.0) and then switched to using
    # an array of objects (with v0.24.0). This rewrites the constraints into the new format as needed.
    constraints = table_design.get("constraints")
    if isinstance(constraints, dict):
        table_design["constraints"] = [
            {constraint_type: constraints[constraint_type]} for constraint_type in sorted(constraints)
        ]

    return validate_table_design(table_design, table_name)


def load_table_design_from_localfile(local_filename, table_name):
    """
    Load (and validate) table design file in local file system.
    """
    if local_filename is None:
        raise ValueError("local filename is unknown")
    try:
        with open(local_filename) as f:
            table_design = load_table_design(f, table_name)
    except Exception:
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
    try:
        validate_table_design_syntax(table_design, table_name)
        validate_table_design_semantics(table_design, table_name)
    except Exception:
        logger.error("Failed to validate table design for '%s'", table_name.identifier)
        raise
    return table_design


def validate_table_design_syntax(table_design, table_name):
    """
    Validate table design based on the (JSON) schema (which can only check syntax but not values).
    Raise an exception if anything is amiss.
    """
    try:
        etl.config.validate_with_schema(table_design, "table_design.schema")
    except SchemaValidationError as exc:
        raise TableDesignSyntaxError("failed to validate table design for '{}'".format(table_name.identifier)) from exc


def validate_identity_as_surrogate_key(table_design):
    """
    Check whether specification of our identity column is valid and whether it matches surrogate key.
    """
    identity_columns = []
    for column in table_design["columns"]:
        if column.get("identity"):
            if not column.get("not_null"):
                # NULL columns may not be primary key (identity)
                raise TableDesignSemanticError("identity column must be set to not null")
            if identity_columns:
                raise TableDesignSemanticError("only one column should have identity")
            identity_columns.append(column["name"])

    constraints = table_design.get("constraints", [])
    surrogate_keys = [col for constraint in constraints for col in constraint.get("surrogate_key", [])]
    if len(surrogate_keys) and not surrogate_keys == identity_columns:
        raise TableDesignSemanticError("surrogate key must be identity column")
    # TODO Complain if surrogate_key is missing but identity is present


def validate_column_references(table_design):
    """
    Make sure that table attributes and constraints only reference columns that actually exist.
    """
    column_list_references = [
        ("constraints", "primary_key"),
        ("constraints", "natural_key"),
        ("constraints", "surrogate_key"),
        ("constraints", "unique"),
        ("attributes", "interleaved_sort"),
        ("attributes", "compound_sort"),
    ]
    valid_columns = frozenset(column["name"] for column in table_design["columns"] if not column.get("skipped"))

    constraints = table_design.get("constraints", [])
    for obj, key in column_list_references:
        if obj == "constraints":
            # This evaluates all unique constraints at once by concatenating all of the columns.
            cols = [col for constraint in constraints for col in constraint.get(key, [])]
        else:  # 'attributes'
            cols = table_design.get(obj, {}).get(key, [])
        unknown = join_with_quotes(frozenset(cols).difference(valid_columns))
        if unknown:
            raise TableDesignSemanticError(
                "{key} columns in {obj} contain unknown column(s): {unknown}".format(obj=obj, key=key, unknown=unknown)
            )


def validate_semantics_of_view(table_design):
    """
    Check for semantics that only apply to views.

    Basically, definitions of views may only contain column names or descriptions.
    Note that validation doesn't catch this since we didn't completely separate
    the schema for CTAS and VIEW but have that distinction only on the source_name.
    """
    # This error occurs when you change from CTAS to VIEW and then forget to remove the extra
    # information for the columns, like type or sql_type.
    for column in table_design["columns"]:
        unwanted_fields = set(column).difference(("name", "description"))
        if unwanted_fields:
            raise TableDesignSemanticError("too much information for column of a VIEW: {}".format(unwanted_fields))
    for obj in ("constraints", "attributes", "extract_settings"):
        if obj in table_design:
            raise TableDesignSemanticError("{} not supported for a VIEW".format(obj))


def validate_semantics_of_table_or_ctas(table_design):
    """
    Check for semantics that apply to tables that are in source schemas or are a CTAS.
    """
    validate_identity_as_surrogate_key(table_design)
    validate_column_references(table_design)

    # Make sure that constraints other than unique constraint appear only once
    constraints = table_design.get("constraints", [])
    seen_constraint_types = set()
    for constraint in constraints:
        for constraint_type in constraint:
            if constraint_type in seen_constraint_types and constraint_type != "unique":
                raise TableDesignSemanticError("multiple constraints of type {}".format(constraint_type))
            seen_constraint_types.add(constraint_type)


def validate_semantics_of_ctas(table_design):
    """
    Check for semantics that apply only to CTAS.
    """
    validate_semantics_of_table_or_ctas(table_design)
    if "extract_settings" in table_design:
        raise TableDesignSemanticError("Extract settings not supported for transformations")


def validate_semantics_of_table(table_design):
    """
    Check for semantics that apply to tables in source schemas.
    """
    validate_semantics_of_table_or_ctas(table_design)

    if "depends_on" in table_design:
        raise TableDesignSemanticError("upstream table '%s' has dependencies listed" % table_design["name"])

    constraints = table_design.get("constraints", [])
    constraint_types_in_design = [constraint_type for constraint in constraints for constraint_type in constraint]
    for constraint_type in constraint_types_in_design:
        if constraint_type in ("natural_key", "surrogate_key"):
            raise TableDesignSemanticError(
                "upstream table '%s' has unexpected %s constraint" % (table_design["name"], constraint_type)
            )

    [split_by_name] = table_design.get("extract_settings", {}).get("split_by", [None])
    if split_by_name:
        split_by_column = fy.first(fy.where(table_design["columns"], name=split_by_name))
        if split_by_column.get("skipped", False):
            raise TableDesignSemanticError("Split-by column type must not be skipped")
        if not split_by_column.get("not_null", False):
            raise TableDesignSemanticError("Split-by column type must have not-null constraint")
        if split_by_column["type"] == "string":
            if split_by_column["sql_type"].lower() not in ("timestamp", "timestamp without time zone"):
                raise TableDesignSemanticError(
                    "Split-by column must be a timestamp when not numeric, not '{}'".format(split_by_column["sql_type"])
                )
        elif split_by_column["type"] not in ("int", "long"):
            raise TableDesignSemanticError(
                "Split-by column type must be int or long when numeric, not '{}'".format(split_by_column["type"])
            )


def validate_table_design_semantics(table_design, table_name):
    """
    Validate table design against rule set based on values (e.g. name of columns).
    Raise an exception if anything is amiss.
    """
    if table_design["name"] != table_name.identifier:
        raise TableDesignSemanticError("name in table design must match target '{}'".format(table_name.identifier))

    schema = etl.config.get_dw_config().schema_lookup(table_name.schema)

    if table_design["source_name"] == "VIEW":
        validate_semantics_of_view(table_design)
        if schema.is_upstream_source:
            raise TableDesignSemanticError(
                "invalid upstream source '%s' in view '%s'" % (table_name.schema, table_name.identifier)
            )
    elif table_design["source_name"] == "CTAS":
        validate_semantics_of_ctas(table_design)
        if schema.is_upstream_source:
            raise TableDesignSemanticError(
                "invalid source name '%s' in upstream table '%s'" % (table_design["source_name"], table_name.identifier)
            )
    else:
        validate_semantics_of_table(table_design)
        if not schema.is_upstream_source:
            raise TableDesignSemanticError(
                "invalid source name '%s' in transformation '%s'" % (table_design["source_name"], table_name.identifier)
            )
