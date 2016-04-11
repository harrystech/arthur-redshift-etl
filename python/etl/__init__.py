from collections import namedtuple
from fnmatch import fnmatch
import os


def env_value(name):
    if name not in os.environ:
        raise ValueError("Environment variable %s not set" % name)
    return os.environ[name]


class TableName(namedtuple('_TableName', ['schema', 'table'])):
        __slots__ = ()

        @property
        def identifier(self):
            return "{0}.{1}".format(*self)

        def __str__(self):
            return '"{0}"."{1}"'.format(*self)


class TableNamePattern(namedtuple('_TableNamePattern', ['schema', 'table'])):
    """
    Split pattern into schema and table pattern.
    (1) If pattern is empty (None), then both, schema and table, are None.
    (2) If pattern is a single identifier (not a pattern), then schema is
    set to that word and table is None.  (This allows to select a single
    upstream source.)
    (3) If pattern has a '.', the pattern are the parts to the left and right
    of '.'.  Again, schema may not be a pattern.
    (4) Everything else is an error

    >>> tnp = TableNamePattern(None)
    >>> tnp.schema, tnp.table
    (None, None)
    >>> tnp = TableNamePattern("www")
    >>> tnp.schema, tnp.table
    ('www', None)
    >>> tnp = TableNamePattern("www.orders*")
    >>> tnp.schema, tnp.table
    ('www', 'orders*')
    >>> tnp = TableNamePattern("w?w")
    Traceback (most recent call last):
    ValueError: Schema must be a literal, not pattern
    >>> tnp = TableNamePattern("w*.orders")
    Traceback (most recent call last):
    ValueError: Schema must be a literal, not pattern
    """
    __slots__ = ()

    def __new__(cls, pattern):
        if pattern is None:
            return super(TableNamePattern, cls).__new__(cls, None, None)
        elif '.' not in pattern:
            if '*' in pattern or '?' in pattern:
                raise ValueError("Schema must be a literal, not pattern")
            return super(TableNamePattern, cls).__new__(cls, pattern, None)
        else:
            schema, table = pattern.split('.', 1)
            if '*' in schema or '?' in schema:
                raise ValueError("Schema must be a literal, not pattern")
            return super(TableNamePattern, cls).__new__(cls, schema, table)

    def match_schema(self, schema):
        return self.schema is None or schema == self.schema

    def match_table(self, table):
        return self.table is None or fnmatch(table, self.table)

    def match(self, table_name):
        return self.match_schema(table_name.schema) and self.match_table(table_name.table)


class ColumnDefinition(namedtuple('_ColumnDefinition',
                                  ['name', 'type', 'sql_type', 'source_sql_type', 'expression', 'not_null'])):
    __slots__ = ()
