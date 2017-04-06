"""
Utility methods to deal with "names" of relations.

To be safe, we always delimit names in queries but would prefer not to during logging. See TableName.

There are additional methods and classes here to support the feature of choosing relations
by a pattern from the command line.

Finally, we offer some context managers to create temporary views and tables that return the
names of the created views or tables and automatically clean up.
"""

import fnmatch


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


def join_column_list(columns):
    """
    Return string with comma-separated, delimited column names
    """
    return ", ".join('"{}"'.format(column) for column in columns)


class TableName:
    """
    Class to automatically create delimited identifiers for tables.

    Given a table s.t, then the cautious identifier for SQL code is: "s"."t"
    But the more readable name is still: s.t

    Another, more curious use is to store shell patterns for the schema name
    and table name so that we can match against other instances.
    """

    __slots__ = ("_schema", "_table")

    def __init__(self, schema, table):
        self._schema = schema.lower()
        self._table = table.lower()

    @property
    def schema(self):
        return self._schema

    @property
    def table(self):
        return self._table

    @property
    def identifier(self):
        """
        Return simple identifier, like one would use on the command line.

        >>> tn = TableName("hello", "world")
        >>> tn.identifier
        'hello.world'
        """
        return "{0.schema}.{0.table}".format(self)

    @classmethod
    def from_identifier(cls, identifier):
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
        """
        return '"{0}"."{1}"'.format(self.schema, self.table)

    def __lt__(self, other):
        """
        Order (or sort) two table names.

        >>> ta = TableName("iowa", "cedar rapids")
        >>> tb = TableName("iowa", "desmoines")
        >>> ta < tb
        True
        """
        return self.identifier < other.identifier

    def match(self, other):
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
        other_schema = other.schema.lower()
        other_table = other.table.lower()
        return fnmatch.fnmatch(other_schema, self.schema) and fnmatch.fnmatch(other_table, self.table)

    def match_pattern(self, pattern):
        """
        Test whether this table matches the given pattern

        >>> tn = TableName("www", "orders")
        >>> tn.match_pattern("w*.o*")
        True
        >>> tn.match_pattern("o*.w*")
        False
        """
        return fnmatch.fnmatch(self.identifier, pattern)

    @staticmethod
    def join_with_quotes(table_names):
        """
        Prettify a list of table names, usually for log statements.

        >>> my_tables = [TableName("www", "orders"), TableName("www", "users")]
        >>> TableName.join_with_quotes(my_tables)
        "'www.orders', 'www.users'"
        """
        return join_with_quotes(sorted(table.identifier for table in table_names))


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

    __slots__ = ["_patterns", "_base_schemas"]

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
        'www.orders*'
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
        elif len(self._patterns) == 1:
            return self._patterns[0].identifier
        else:
            return "[{}]".format(TableName.join_with_quotes(self._patterns))

    def match_schema(self, schema):
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
