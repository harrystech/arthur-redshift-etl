import logging
import re

import simplejson as json

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Attribute:
    """
    Most basic description of a database "attribute", a.k.a. column.

    Attributes are purely based on information that we find in upstream databases.
    """

    __slots__ = ("name", "sql_type", "not_null")

    def __init__(self, name, sql_type, not_null):
        self.name = name
        self.sql_type = sql_type
        self.not_null = not_null


class ColumnDefinition:
    """
    More granular description of a column in a table.

    These are ready to be sent to a table design or come from a table design file.
    """

    __slots__ = ("name", "type", "sql_type", "source_sql_type", "expression", "not_null")

    def __init__(self, name, source_sql_type, sql_type, expression, type_, not_null):
        self.name = name
        self.source_sql_type = source_sql_type
        self.sql_type = sql_type
        self.expression = expression
        self.type = type_
        self.not_null = not_null

    def to_dict(self):
        d = dict(name=self.name, sql_type=self.sql_type, type=self.type)
        if self.expression is not None:
            d["expression"] = self.expression
        if self.source_sql_type != self.sql_type:
            d["source_sql_type"] = self.source_sql_type
        if self.not_null:
            d["not_null"] = self.not_null
        return d

    @staticmethod
    def from_attribute(attribute, as_is_att_type, cast_needed_att_type, default_att_type):
        """
        Turn a table attribute into a "column" of a table design.

        This adds the generic type and possibly a cast into a supported type.
        """
        for re_att_type, generic_type in as_is_att_type.items():
            if re.match("^" + re_att_type + "$", attribute.sql_type):
                # Keep the type, use no expression, and pick generic type from map.
                mapping_sql_type, mapping_expression, mapping_type = attribute.sql_type, None, generic_type
                break
        else:
            for re_att_type, (mapping_sql_type, mapping_expression, mapping_type) in cast_needed_att_type.items():
                if re.match(re_att_type, attribute.sql_type):
                    # Found tuple with new SQL type, expression and generic type.  Rejoice.
                    break
            else:
                logger.warning(
                    "Unknown type '{}' of column '{}' (using default)".format(attribute.sql_type, attribute.name)
                )
                mapping_sql_type, mapping_expression, mapping_type = default_att_type

        delimited_name = '"{}"'.format(attribute.name)
        return ColumnDefinition(
            attribute.name,
            attribute.sql_type,
            mapping_sql_type,
            # Replace %s in the column expression by the column name.
            (mapping_expression % delimited_name if mapping_expression else None),
            mapping_type,
            attribute.not_null,
        )


class TableDesign:
    """Placeholder until we turn dict-based table designs into a class."""

    @staticmethod
    def make_item_sorter():
        """
        Return function that allows sorting keys that appear in any "object" (JSON-speak for dict).

        The sort order makes the resulting order of keys easier to digest by humans.

        Input to the sorter is a tuple of (key, value) from turning a dict into a list of items.
        Output (return value) of the sorter is a tuple of (preferred order, key name).
        If a key is not known, it's sorted alphabetically (ignoring case) after all known ones.
        """
        preferred_order = [
            # always (tables, columns, etc.)
            "name",
            "description",
            # only tables
            "source_name",
            "unload_target",
            "depends_on",
            "constraints",
            "attributes",
            "columns",
            # only columns
            "sql_type",
            "type",
            "expression",
            "source_sql_type",
            "not_null",
            "identity",
        ]
        order_lookup = {key: (i, key) for i, key in enumerate(preferred_order)}
        max_index = len(preferred_order)

        def sort_key(item):
            key, value = item
            return order_lookup.get(key, (max_index, key))

        return sort_key

    @staticmethod
    def as_string(table_design: dict) -> str:
        # We use JSON pretty printing because it is prettier than YAML printing.
        return json.dumps(table_design, indent="    ", item_sort_key=TableDesign.make_item_sorter()) + "\n"
