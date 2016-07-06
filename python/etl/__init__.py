""""
Utilities and classes to support the ETL in general
"""

from contextlib import contextmanager
from collections import namedtuple
from datetime import datetime
from fnmatch import fnmatch
import logging


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

    def match_schema(self, schema):
        """
        Match against schema.

        >>> tnp = TableNamePatterns.from_list(["www.orders", "www.products"])
        >>> tnp.match_schema("www")
        True
        >>> tnp.match_schema("finance")
        False
        """
        return not self.schemas or schema.lower() in self.schemas

    def match_table(self, table):
        """
        Pattern match against table.

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

    def match_names(self, sources: list) -> list:
        """
        Match names with this pattern while traversing sources where each element is a dictionary having a "name" key.
        """
        return list(d["name"] for d in sources if self.match_schema(d["name"]))


class ColumnDefinition(namedtuple("_ColumnDefinition",
                                  ["name",  # always
                                   "type", "sql_type",  # always for tables
                                   "source_sql_type", "expression", "not_null", "references"  # optional
                                   ])):
    """
    Wrapper for column attributes ... describes columns by name, type (for Avro), sql_type.
    """
    __slots__ = ()


class AssociatedTableFiles():
    """
    Class to hold design file, SQL file and data files (CSV and manifest) belonging to a table.

    Note that tables are addressed using their target name, where the schema is equal
    to the source name.  To allow for sorting, the original schema name (in the source
    database) is kept.  For views and CTAS, the sort order is used to create a predictable
    instantiation order where one view or CTAS may depend on another being up-to-date already.
    """

    def __init__(self, source_table_name, target_table_name, design_file):
        self._source_table_name = source_table_name
        self._target_table_name = target_table_name
        self._design_file = design_file
        self._sql_file = None
        self._manifest_file = None
        self._data_files = []

    def __str__(self):
        return "{}({}:{},{},{}{})".format(self.__class__.__name__,
                                          self.source_path_name,
                                          self.design_file,
                                          self._sql_file,
                                          self._manifest_file,
                                          self._data_files)

    @property
    def target_table_name(self):
        return self._target_table_name

    @property
    def source_table_name(self):
        return self._source_table_name

    @property
    def source_path_name(self):
        return "{}/{}-{}".format(self._target_table_name.schema,
                                 self._source_table_name.schema,
                                 self._source_table_name.table)

    @property
    def design_file(self):
        return self._design_file

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def manifest_file(self):
        return self._manifest_file

    @property
    def data_files(self):
        return self._data_files

    def set_sql_file(self, filename):
        self._sql_file = filename

    def set_manifest_file(self, filename):
        self._manifest_file = filename

    def add_data_file(self, filename):
        self._data_files.append(filename)

    def create_manifest(self, bucket_name):
        if len(self._data_files) == 0:
            return None
        else:
            return {
                "entries": [
                    {
                        "mandatory": True,
                        "url": "s3://{}/{}".format(bucket_name, filename)
                    } for filename in self._data_files
                ]
            }


@contextmanager
def measure_elapsed_time():
    """
    Measure time it takes to execute code and report on success.

    Exceptions are being caught here and reported.

    Example:
        >>> with measure_elapsed_time():
        ...     pass
    """
    def elapsed_time(start_time=datetime.now()):
        return (datetime.now() - start_time).total_seconds()

    # For some weird reason, this does NOT work: logger = logging.getLogger(__name__)
    try:
        yield
    except Exception:
        logging.getLogger(__name__).exception("Something terrible happened")
        logging.getLogger(__name__).info("Ran for %.2fs before encountering disaster!", elapsed_time())
        raise
    except BaseException:
        logging.getLogger(__name__).exception("Something really terrible happened")
        logging.getLogger(__name__).info("Ran for %.2fs before an exceptional termination!", elapsed_time())
        raise
    else:
        logging.getLogger(__name__).info("Ran for %.2fs and finished successfully!", elapsed_time())
