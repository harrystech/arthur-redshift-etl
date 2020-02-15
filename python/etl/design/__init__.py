import logging
import re

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
        Turn a table attribute into a "column" of a table design. This adds the generic type and
        possibly a cast into a supported type.
        """
        for re_att_type, generic_type in as_is_att_type.items():
            if re.match('^' + re_att_type + '$', attribute.sql_type):
                # Keep the type, use no expression, and pick generic type from map.
                mapping_sql_type, mapping_expression, mapping_type = attribute.sql_type, None, generic_type
                break
        else:
            for re_att_type, (mapping_sql_type, mapping_expression, mapping_type) in cast_needed_att_type.items():
                if re.match(re_att_type, attribute.sql_type):
                    # Found tuple with new SQL type, expression and generic type.  Rejoice.
                    break
            else:
                logger.warning("Unknown type '{}' of column '{}' (using default)".format(attribute.sql_type,
                                                                                         attribute.name))
                mapping_sql_type, mapping_expression, mapping_type = default_att_type

        delimited_name = '"{}"'.format(attribute.name)
        return ColumnDefinition(attribute.name,
                                attribute.sql_type,
                                mapping_sql_type,
                                # Replace %s in the column expression by the column name.
                                (mapping_expression % delimited_name if mapping_expression else None),
                                mapping_type,
                                attribute.not_null)
