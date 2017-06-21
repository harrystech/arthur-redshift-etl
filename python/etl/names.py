"""
Utility methods to deal with "names" of relations.

To be safe, we always delimit names in queries but would prefer not to during logging. See TableName.

There are additional methods and classes here to support the feature of choosing relations
by a pattern from the command line.
"""

import fnmatch
import uuid
from typing import Optional, List


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
    >>> join_with_quotes(frozenset(["foo", "bar"]))
    "'bar', 'foo'"
    """
    if isinstance(names, (set, frozenset)):
        return ', '.join("'{}'".format(name) for name in sorted(names))
    else:
        return ', '.join("'{}'".format(name) for name in names)


def join_column_list(columns):
    """
    Return string with comma-separated, delimited column names
    """
    return ", ".join('"{}"'.format(column) for column in columns)


def as_staging_name(name):
    """
    The canonical transformation of a (schema) name to its staging position
    """
    return '$'.join(("etl_staging", name))


def as_backup_name(name):
    """
    The canonical transformation of a (schema) name to its backup position
    """
    return '$'.join(("etl_backup", name))


class TableName:
    """
    Class to automatically create delimited identifiers for tables.

    Given a table s.t, then the cautious identifier for SQL code is: "s"."t"
    But the more readable name is still: s.t

    Another, more curious use is to store shell patterns for the schema name
    and table name so that we can match against other instances.

    Comparisons (for schema and table names) are case-insensitive.

    >>> orders = TableName.from_identifier("www.orders")
    >>> str(orders)
    '"www"."orders"'
    >>> orders.identifier
    'www.orders'
    >>> same_orders = TableName.from_identifier("WWW.Orders")
    >>> orders == same_orders
    True
    >>> id(orders) == id(same_orders)
    False
    >>> hash(orders) == hash(same_orders)
    True
    >>> w3 = TableName.from_identifier("w3.orders")
    >>> orders == w3
    False
    >>> purchases = TableName.from_identifier("www.purchases")
    >>> orders < purchases
    True
    >>> staging_purchases = purchases.as_staging_table_name()
    >>> staging_purchases.table == purchases.table
    True
    >>> staging_purchases.schema == purchases.schema
    False
    """

    __slots__ = ("_schema", "_table", "_staging")

    def __init__(self, schema: Optional[str], table: str) -> None:
        # Concession to subclasses ... schema is optional
        self._schema = schema.lower() if schema else None
        self._table = table.lower()
        self._staging = False

    @property
    def schema(self):
        if self.staging and not self._schema.startswith('pg_catalog'):
            return as_staging_name(self._schema)
        else:
            return self._schema

    @property
    def table(self):
        return self._table

    @property
    def staging(self):
        return self._staging

    def to_tuple(self):
        """
        Return schema name and table name as a handy tuple.

        >>> tn = TableName("weather", "temp")
        >>> schema_name, table_name = tn.to_tuple()
        >>> schema_name, table_name
        ('weather', 'temp')
        """
        return self.schema, self.table

    @property
    def identifier(self) -> str:
        """
        Return simple identifier, like one would use on the command line.

        >>> tn = TableName("hello", "world")
        >>> tn.identifier
        'hello.world'
        """
        return "{}.{}".format(*self.to_tuple())

    @classmethod
    def from_identifier(cls, identifier: str):
        """
        Split identifier into schema and table before creating a new TableName instance

        >>> identifier = "ford.mustang"
        >>> tn = TableName.from_identifier(identifier)
        >>> identifier == tn.identifier
        True
        """
        schema, table = identifier.split('.', 1)
        return cls(schema, table)

    def __str__(self):
        """
        Delimited table identifier to safeguard against unscrupulous users who use "default" as table name...

        >>> tn = TableName("hello", "world")
        >>> str(tn)
        '"hello"."world"'
        >>> str(tn.as_staging_table_name())
        '"etl_staging$hello"."world"'
        """
        return '"{}"."{}"'.format(*self.to_tuple())

    def __format__(self, code):
        """
        Format name as delimited identifier (by default, or 's') or just as identifier (using 'x').

        >>> pu = TableName("public", "users")
        >>> format(pu)
        '"public"."users"'
        >>> format(pu, 'x')
        'public.users'
        >>> "SELECT * FROM {:s}".format(pu)
        'SELECT * FROM "public"."users"'
        >>> "Table '{:x}' contains users".format(pu)  # new style with using formatting code
        "Table 'public.users' contains users"
        >>> "Table '{}' contains users".format(pu.identifier)  # old style by accessing property
        "Table 'public.users' contains users"
        >>> "Oops: {:y}".format(pu)
        Traceback (most recent call last):
        ValueError: Unknown format code 'y' for TableName
        """
        if (not code) or (code == 's'):
            return str(self)
        elif code == 'x':
            return self.identifier
        else:
            raise ValueError("Unknown format code '{}' for {}".format(code, self.__class__.__name__))

    def __eq__(self, other: object):
        if isinstance(other, TableName):
            return self.to_tuple() == other.to_tuple()
        else:
            return False

    def __hash__(self):
        return hash(tuple(getattr(self, slot) for slot in self.__slots__))

    def __lt__(self, other: "TableName"):
        """
        Order two table names, case-insensitive. (Used by sort.)

        >>> ta = TableName("Iowa", "Cedar Rapids")
        >>> tb = TableName("Iowa", "Desmoines")
        >>> ta < tb
        True
        """
        return self.identifier < other.identifier

    def match(self, other: "TableName") -> bool:
        """
        Treat yo'self as a tuple of patterns and match against the other table.

        >>> tp = TableName("w*", "o*")
        >>> tn = TableName("www", "orders")
        >>> tp.match(tn)
        True
        >>> tn = TableName("worldwide", "octopus")
        >>> tp.match(tn)
        True
        >>> tn = TableName("sales", "orders")
        >>> tp.match(tn)
        False
        """
        other_schema = other.schema
        other_table = other.table
        return fnmatch.fnmatch(other_schema, self.schema) and fnmatch.fnmatch(other_table, self.table)

    def match_pattern(self, pattern: str) -> bool:
        """
        Test whether this table matches the given pattern

        >>> tn = TableName("www", "orders")
        >>> tn.match_pattern("w*.o*")
        True
        >>> tn.match_pattern("o*.w*")
        False
        """
        return fnmatch.fnmatch(self.identifier, pattern)

    def as_staging_table_name(self):
        tn = TableName(*self.to_tuple())
        tn._staging = True
        return tn


class TempTableName(TableName):
    """
    Class to deal with names of temporary relations.

    Note that temporary views or tables do not have a schema (*) and have a name starting with '#'.
    (* = strictly speaking, their schema is one of the pg_temp% schemas. But who's looking.)

    >>> temp = TempTableName("#hello")
    >>> str(temp)
    '"#hello"'
    >>> temp.identifier
    '#hello'
    >>> "For SQL: {:s}, for logging: {:x}".format(temp, temp)
    'For SQL: "#hello", for logging: #hello'

    Schema and name comparison in SQL continues to work if you use LIKE for schema names:
    >>> temp.schema
    'pg_temp%'
    """
    def __init__(self, table) -> None:
        if not table.startswith('#'):
            raise ValueError("name of temporary table must start with '#'")
        super().__init__(None, table)

    @property
    def schema(self):
        return "pg_temp%"

    @property
    def identifier(self):
        return self.table

    def __str__(self):
        return '"{}"'.format(self.table)

    @staticmethod
    def for_table(table: TableName):
        """
        Return a valid name for a temporary table that's derived from the given table name.

        Leaks Redshift spec in that we make sure that names are less than 127 characters long.

        >>> table = "public.speakeasy"
        >>> tn = TableName.from_identifier(table)
        >>> temp = TempTableName.for_table(tn)
        >>> temp.identifier
        '#public$speakeasy'
        >>> str(temp)
        '"#public$speakeasy"'

        >>> too_long = "public." + "abcd" * 32
        >>> tt = TempTableName.for_table(TableName.from_identifier(too_long))
        >>> len(tt.identifier)
        127
        """
        temp_name = "#{0.schema}${0.table}".format(table)
        if len(temp_name) > 127:
            temp_name = temp_name[:119] + uuid.uuid4().hex[:8]
        return TempTableName(temp_name)


class TableSelector:
    """
    Class to hold patterns to filter table names.

    Patterns that are supported are based on "glob" matches, which use *, ?, and [] -- just
    like the shell does. But note that all matching is done case-insensitive.

    There is a concept of "base schemas."  This list should be based on the configuration and defines
    the set of usable schemas.  ("Schemas" here refers to either upstream sources or schemas storing
    transformations.) So when base schemas are defined then there is an implied additional
    match against them before a table name is tried to be matched against stored patterns.
    If no base schemas are set, then we default simply to a list of schemas from the patterns.
    """

    __slots__ = ("_patterns", "_base_schemas")

    def __init__(self, patterns=None, base_schemas=None):
        """
        Build pattern instance from list of glob patterns.

        The list may be empty (or omitted).  This is equivalent to a list of ["*.*"].
        Note that each pattern is split on the first '.' to separate out
        matches against schema names and table names.
        To facilitate case-insensitive matching, patterns are stored in their
        lower-case form.

        The base schemas (if present) basically limit what a '*' means as schema name.
        They are stored in their initial order.

        >>> ts = TableSelector()
        >>> str(ts)
        '*.*'
        >>> ts = TableSelector(["finance", "www"])
        >>> str(ts)
        "['finance.*', 'www.*']"
        >>> ts = TableSelector(["www.orders*"])
        >>> str(ts)
        "['www.orders*']"
        >>> ts = TableSelector(["www.Users", "www.Products"])
        >>> str(ts)
        "['www.products', 'www.users']"
        >>> ts = TableSelector(["*.orders", "finance.budget"])
        >>> str(ts)
        "['*.orders', 'finance.budget']"
        >>> ts = TableSelector("www.orders")
        Traceback (most recent call last):
        ValueError: patterns must be a list

        >>> ts = TableSelector(["www.*", "finance"], ["www", "finance", "operations"])
        >>> ts.base_schemas
        ('www', 'finance', 'operations')
        >>> ts.base_schemas = ["www", "marketing"]
        Traceback (most recent call last):
        ValueError: bad pattern (no match against base schemas): finance.*
        >>> ts.base_schemas = ["www", "finance", "marketing"]

        >>> ts = TableSelector(base_schemas=["www"])
        >>> ts.match(TableName.from_identifier("www.orders"))
        True
        >>> ts.match(TableName.from_identifier("operations.shipments"))
        False
        """
        if patterns is None:
            patterns = []  # avoid having a modifiable parameter but still have a for loop
        if not isinstance(patterns, list):
            raise ValueError("patterns must be a list")

        split_patterns = []
        for pattern in patterns:
            if '.' in pattern:
                schema, table = pattern.split('.', 1)
                split_patterns.append(TableName(schema, table))
            else:
                split_patterns.append(TableName(pattern, '*'))
        self._patterns = tuple(sorted(split_patterns))

        self._base_schemas = ()
        if base_schemas is not None:
            self.base_schemas = base_schemas

    @property
    def base_schemas(self):
        return self._base_schemas

    @base_schemas.setter
    def base_schemas(self, schemas):
        """
        Add base schemas (names, not patterns) to match against.
        It is an error to have a pattern that does not match against the base schemas.
        """
        # Fun fact: you can't have doctests in docstrings for properties
        self._base_schemas = tuple(name.lower() for name in schemas)

        # Make sure that each pattern matches against at least one base schema
        for pattern in self._patterns:
            found = fnmatch.filter(self._base_schemas, pattern.schema)
            if not found:
                raise ValueError("bad pattern (no match against base schemas): {}".format(pattern.identifier))

    def __str__(self):
        # See __init__ for tests
        if len(self._patterns) == 0:
            return '*.*'
        else:
            return "[{}]".format(join_with_quotes(p.identifier for p in self._patterns))

    def match_schema(self, schema) -> bool:
        """
        Match against schema name, return true if any pattern matches the schema name
        and the schema is part of the base schemas (if defined).

        >>> tnp = TableSelector(["www.orders", "factory.products"])
        >>> tnp.match_schema("www")
        True
        >>> tnp.match_schema("finance")
        False
        """
        name = schema.lower()
        if not self._patterns:
            if not self._base_schemas:
                return True
            else:
                return name in self._base_schemas
        else:
            for pattern in self._patterns:
                if fnmatch.fnmatch(name, pattern.schema):
                    return True
            return False

    def selected_schemas(self) -> List[str]:
        """
        Return list of schemas from base schemas that match the selection.
        It is an error if a pattern tries to select a specific table instead of a schema.

        >>> ts = TableSelector(["www.*", "marketing"], ["factory", "marketing", "www"])
        >>> ts.selected_schemas()
        ['marketing', 'www']
        >>> tx = TableSelector(["www.orders"], ["www"])
        >>> tx.selected_schemas()
        Traceback (most recent call last):
        ValueError: pattern selects table, not schema: '"www"."orders"'
        """
        for pattern in self._patterns:
            if pattern.table != '*':
                raise ValueError("pattern selects table, not schema: '%s'" % pattern)
        return [schema for schema in self._base_schemas if self.match_schema(schema)]

    def match(self, table_name):
        """
        Match names of schema and table against known patterns, return true if any pattern matches
        and the schema is part of the base schemas (if defined).

        >>> ts = TableSelector(["www.orders", "www.prod*"])
        >>> name = TableName("www", "products")
        >>> ts.match(name)
        True
        >>> name = TableName("WWW", "Products")
        >>> ts.match(name)
        True
        >>> name = TableName("finance", "products")
        >>> ts.match(name)
        False
        >>> name = TableName("www", "users")
        >>> ts.match(name)
        False
        """
        schema = table_name.schema
        if self._base_schemas and schema not in self._base_schemas:
            return False
        if not self._patterns:
            return True
        for pattern in self._patterns:
            if pattern.match(table_name):
                return True
        return False
