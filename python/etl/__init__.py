from collections import namedtuple
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


class ColumnDefinition(namedtuple('_ColumnDefinition',
                                  ['name', 'type', 'sql_type', 'source_sql_type', 'expression', 'not_null'])):
    __slots__ = ()
