""""
Utilities and classes to support the ETL in general
"""

from collections import namedtuple
from fnmatch import fnmatch

import pkg_resources


# TODO rename package to "redshift_etl" and rename python module to "redshift_etl"
def package_version(package_name="redshift-etl"):
    return "{} v{}".format(package_name, pkg_resources.get_distribution(package_name).version)


def join_with_quotes(names):
    """
    Individually wrap the names in quotes and return comma-separated names in a string.
    If the input is a set of names, the names are sorted first.
    If the input is a list of names, the order of the list is respected.
    If the input is cheese, the order is for more red wine.


    >>> join_with_quotes(["foo", "bar"])
    "'foo', 'bar'"
    >>> join_with_quotes({"foo", "bar"})
    "'bar', 'foo'"
    """
    if isinstance(names, (set, frozenset)):
        return ', '.join("'{}'".format(name) for name in sorted(names))
    else:
        return ', '.join("'{}'".format(name) for name in names)


class ETLException(Exception):
    """Parent to all ETL-oriented exceptions which allows to write effective except statements"""
    pass


class TableName(namedtuple("_TableName", ["schema", "table"])):
    """
    Class to automatically create delimited identifiers for table.

    Given a table s.t, then the cautious identifier for SQL code is: "s"."t"
    But the more readable name is still: s.t
    """

    __slots__ = ()

    @property
    def identifier(self):
        return "{0}.{1}".format(self.schema, self.table)

    def __str__(self):
        return '"{0}"."{1}"'.format(self.schema, self.table)

    def match(self, glob):
        return fnmatch(self.identifier, glob)

    @staticmethod
    def join_with_quotes(table_names):
        return join_with_quotes(sorted(table.identifier for table in table_names))


class TableNamePatterns(namedtuple("_TableNamePattern", ["schemas", "table_patterns"])):
    """
    Class to hold patterns to filter schemas and tables.
    """

    __slots__ = ()

    @classmethod
    def from_list(cls, patterns):
        """
        Split pattern into (list of) schemas and (list of) table patterns.

        (1) If pattern list is empty ([]), then schemas is [] and table_patterns
        is [].  This will match anything.
        (2) If any pattern has a '.', then the left hand side is the schema (and
        must be the same for all patterns) and the right hand side is a pattern.
        This will match any table that matches any of the patterns in the
        selected schema.
        (3) If pattern list is list of single identifiers (no patterns, no '.'),
        then schemas is set to that list and table_patterns is [].  This allows
        to select multiple upstream sources and matches every table within the
        corresponding schemas.
        (4) Everything else is an error.

        NB. The two lists, schemas and table_patterns, will never both have more
        than one element.

        You should use the convenience methods (match_schema, match_table) in
        order not to have to know these nuances.  Just feed it the argument
        list from a command line option with nargs='*' or nargs='+'.
        To facilitate case-insensitive matching, patterns are stored in their
        lower-case form.

        >>> tnp = TableNamePatterns.from_list([])
        >>> str(tnp)
        '*.*'
        >>> tnp.schemas, tnp.table_patterns
        ([], [])
        >>> tnp = TableNamePatterns.from_list(["www"])
        >>> str(tnp)
        'www.*'
        >>> tnp.schemas, tnp.table_patterns
        (['www'], [])
        >>> tnp = TableNamePatterns.from_list(["www.orders*"])
        >>> str(tnp)
        'www.orders*'
        >>> tnp.schemas, tnp.table_patterns
        (['www'], ['orders*'])
        >>> tnp = TableNamePatterns.from_list(["www.Users", "www.Products"])
        >>> str(tnp)
        'www.[users,products]'
        >>> tnp.schemas, tnp.table_patterns
        (['www'], ['users', 'products'])
        >>> tnp = TableNamePatterns.from_list(["www", "finance"])
        >>> str(tnp)
        '[www.*,finance.*]'
        >>> tnp.schemas, tnp.table_patterns
        (['www', 'finance'], [])
        >>> tnp = TableNamePatterns.from_list(["w?w"])
        Traceback (most recent call last):
        ValueError: Schema must be a literal, not pattern
        >>> tnp = TableNamePatterns.from_list(["w*.orders"])
        Traceback (most recent call last):
        ValueError: Schema must be a literal, not pattern
        >>> tnp = TableNamePatterns.from_list(["www.orders", "finance.budget"])
        Traceback (most recent call last):
        ValueError: Schema must be same
        >>> tnp = TableNamePatterns.from_list(["www.orders", "www"])
        Traceback (most recent call last):
        ValueError: Cannot mix schema and table selection
        >>> tnp = TableNamePatterns.from_list("www.orders")
        Traceback (most recent call last):
        ValueError: Patterns must be a list
        """
        if not isinstance(patterns, list):
            raise ValueError("Patterns must be a list")
        elif len(patterns) == 0:
            return cls([], [])
        elif all('.' in pattern for pattern in patterns):
            selected_schema = None
            table_patterns = []
            for schema, table in [pattern.lower().split('.', 1) for pattern in patterns]:
                if '*' in schema or '?' in schema:
                    raise ValueError("Schema must be a literal, not pattern")
                if selected_schema is None:
                    selected_schema = schema
                elif selected_schema != schema:
                    raise ValueError("Schema must be same")
                table_patterns.append(table)
            return cls([selected_schema], table_patterns)
        elif any('.' in pattern for pattern in patterns):
            raise ValueError("Cannot mix schema and table selection")
        else:
            if any('*' in schema or '?' in schema for schema in patterns):
                raise ValueError("Schema must be a literal, not pattern")
            return cls([schema.lower() for schema in patterns], [])

    def __str__(self):
        if len(self.schemas) == 0:
            return '*.*'
        elif len(self.schemas) > 1:
            return '[{}]'.format(','.join('{}.*'.format(schema) for schema in self.schemas))
        elif len(self.table_patterns) == 0:
            return "{}.*".format(self.schemas[0])
        elif len(self.table_patterns) == 1:
            return "{}.{}".format(self.schemas[0], self.table_patterns[0])
        else:
            return "{}.[{}]".format(self.schemas[0], ','.join(self.table_patterns))

    def str_schemas(self):
        if len(self.schemas) == 0:
            return '*'
        elif len(self.schemas) == 1:
            return str(self.schemas[0])
        else:
            return str(self.schemas)

    def match_schema(self, schema):
        """
        Match against schema name.

        >>> tnp = TableNamePatterns.from_list(["www.orders", "www.products"])
        >>> tnp.match_schema("www")
        True
        >>> tnp.match_schema("finance")
        False
        """
        return not self.schemas or schema.lower() in self.schemas

    def match_table(self, table):
        """
        Pattern match against table name.

        >>> tnp = TableNamePatterns.from_list(["www.order?", "www.products"])
        >>> tnp.match_table("orders")
        True
        >>> tnp.match_schema("users")
        False
        """
        name = table.lower()
        return not self.table_patterns or any(fnmatch(name, pattern) for pattern in self.table_patterns)

    def match(self, table_name):
        """
        Match against schema and pattern match against table.

        >>> tnp = TableNamePatterns.from_list(["www.orders", "www.prod*"])
        >>> name = TableName("www", "products")
        >>> tnp.match(name)
        True
        >>> name = TableName("finance", "products")
        >>> tnp.match(name)
        False
        >>> name = TableName("www", "users")
        >>> tnp.match(name)
        False
        """
        return self.match_schema(table_name.schema) and self.match_table(table_name.table)

    def match_name(self, los):
        """
        Match in the list of objects those that have a matching "name" attribute value.
        """
        return [s for s in los if self.match_schema(s.name)]
