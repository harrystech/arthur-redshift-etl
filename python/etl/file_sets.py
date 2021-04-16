"""
Utilities to interact with Amazon S3 and occasionally the local file system.

The basic location of a file depends on the data source and can be one of:
.../schemas/{source_or_schema_name}/{source_schema_name}-{table_name}.yaml -- for table design files
.../schemas/{schema_name}/{source_schema_name}-{table_name}.sql -- for queries for CTAS or views
.../data/{source_name}/{source_schema_name}-{table_name}.manifest -- for a manifest of data files
.../data/{source_name}/{source_schema_name}-{table_name}/csv/part-*.gz -- for the data files

If the files are in S3, then the start of the path is always s3://{bucket_name}/{prefix}/...
If the files are stored locally, then the start of the path is probably simply the current
directory ('.').

For tables that are backed by upstream sources, the directory after 'schemas' or 'data' will be the
name of the source in the configuration file.

For CTAS or views, the directory after 'schemas' is the name of the schema in the data warehouse
configuration. The 'source_schema_name' is only used for sorting.

Files are logically grouped by the target relation that they describe (or help fill with data).
"""

import logging
import os
import os.path
import re
from datetime import datetime
from itertools import groupby
from operator import attrgetter
from typing import Iterator, Optional

import etl.config
import etl.s3
import etl.text
from etl.errors import ETLSystemError
from etl.names import TableName, TableSelector

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class FileInfo:
    """
    Store file path along with information such as file type, related schema and table names.

    Known file types:
      - yaml: Table design file
      - sql: Query for CTAS or VIEW
      - manifest: Manifest of data files
      - success: Sentinel file to mark complete data
      - columns: Meta-info about columns in data files
      - data: Data file in Avro, CSV, or JSON format (see data_format property).

    Note that manifest files can occur twice, once from the extract as:
        .../data/{source_name}/{source_schema_name}-{table_name}.manifest
    and once after unload:
        .../data/{unload_target}/{unloading_schema_name}-{table_name}/csv/manifest
    """

    filename_re = re.compile(
        r"""(?:^schemas|/schemas|^data|/data)
            /(?P<schema_name>\w+)
            /(?:(?P<source_schema_name>\w+)-)?(?P<table_name>\w+)
            (?:[.](?P<file_ext>yaml|sql|manifest)|
                /(?P<data_dir>avro|csv|json)/(?P<data_file>[^/]+))$
        """,
        re.VERBOSE,
    )

    def __init__(self, filename, schema_name, source_schema_name, table_name, file_type, file_format) -> None:
        self.filename = filename
        self.schema_name = schema_name
        self.source_schema_name = source_schema_name
        self.table_name = table_name
        self.file_type = file_type
        self.file_format = file_format

    @classmethod
    def from_filename(cls, filename: str) -> Optional["FileInfo"]:
        """
        Return a FileInfo instance with properties derived from the filename.

        If the filename doesn't appear to match a file that we can use, return None.
        """
        match = cls.filename_re.search(filename)
        if not match:
            return None

        values = match.groupdict()
        schema_name = values["schema_name"]
        source_schema_name = values["source_schema_name"] or schema_name
        table_name = values["table_name"]

        file_format = None
        file_ext = values["file_ext"]
        if file_ext in ("yaml", "sql", "manifest"):
            file_type = file_ext
        elif values["data_file"] == "_SUCCESS":
            file_type = "success"
        elif values["data_file"] == "columns.yaml":
            file_type = "columns"
        elif values["data_file"] == "manifest":
            file_type = "manifest"
        else:
            file_format = values["data_dir"].upper()
            file_type = "data"

        return cls(filename, schema_name, source_schema_name, table_name, file_type, file_format)


def _find_matching_files_from(iterable, pattern):
    """
    Return file information based on source name, source schema, table name and file type.

    This generator provides all files that are relevant and match the pattern. It
    is up to the consumer to ensure consistency (e.g. that a design
    file exists or that a SQL file is not present along with a manifest).

    Files ending in '_$folder$' are ignored. (They are created by some Spark jobs.)

    >>> found = list(_find_matching_files_from([
    ...     "/schemas/store/public-orders.yaml",
    ...     "/data/store/public-orders.manifest",
    ...     "/data/store/public-orders/csv/_SUCCESS",
    ...     "/data/store/public-orders/csv/part-0.gz",
    ...     "/schemas/dw/orders.sql",
    ...     "/schemas/dw/orders.yaml",
    ...     "/data/events/kinesis-stream/json/part-0.gz",
    ...     "/schemas/store/not-selected.yaml",
    ... ], pattern=TableSelector(["dw.*", "events", "store.orders"])))
    >>> files = {file_info.filename: file_info for file_info in found}
    >>> len(files)
    7
    >>> files["/schemas/store/public-orders.yaml"].file_type
    'yaml'
    >>> files["/schemas/store/public-orders.yaml"].schema_name
    'store'
    >>> files["/schemas/store/public-orders.yaml"].source_schema_name
    'public'
    >>> files["/schemas/store/public-orders.yaml"].table_name
    'orders'
    >>> files["/data/store/public-orders.manifest"].file_type
    'manifest'
    >>> files["/data/store/public-orders/csv/_SUCCESS"].file_type
    'success'
    >>> files["/data/store/public-orders/csv/part-0.gz"].file_type
    'data'
    >>> files["/data/store/public-orders/csv/part-0.gz"].file_format
    'CSV'
    >>> files["/schemas/dw/orders.sql"].schema_name
    'dw'
    >>> files["/schemas/dw/orders.sql"].source_schema_name
    'dw'
    >>> files["/schemas/dw/orders.sql"].table_name
    'orders'
    >>> files["/schemas/dw/orders.sql"].file_type
    'sql'
    >>> files["/schemas/dw/orders.yaml"].file_type
    'yaml'
    >>> files["/data/events/kinesis-stream/json/part-0.gz"].file_format
    'JSON'
    """
    for filename in iterable:
        file_info = FileInfo.from_filename(filename)
        if file_info is None:
            if not filename.endswith("_$folder$"):
                logger.warning("Found file not matching expected format: '%s'", filename)
            continue

        target_table_name = TableName(file_info.schema_name, file_info.table_name)
        if pattern.match(target_table_name):
            yield file_info


class RelationFileSet:
    """
    Class to hold a relation's design file, SQL file and data files (including their manifest).

    RelationFileSet instances have a natural_order based on their schema's position in the
    DataWarehouseConfig's schema list and their source_table_name. (So we try to sort sources
    by their order in the configuration and not alphabetically.)

    Note that all tables are addressed using their "target name" within the data warehouse, where
    the schema name is equal to the source name and the table name is the same as in the upstream
    source. To allow for sorting, the original schema name (in the source database) is kept.
    For CTAS and VIEWs, the "target name" consists of the schema name within
    the data warehouse and the table or view name assigned.  Since there is no source schema, that
    portion may be omitted.
    """

    def __init__(self, source_table_name, target_table_name, natural_order):
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        # Note that a value other than None implies that the file exists (see setters)
        self.design_file_name = None
        self.sql_file_name = None
        self.manifest_file_name = None
        self._data_files = []
        # Used when binding the files to either local filesystem or S3
        self.scheme = None
        self.netloc = None
        self.path = None
        # Used to order relations (should be opaque to users)
        self._natural_order = natural_order

    def __lt__(self, other):
        return self._natural_order < other._natural_order

    def __repr_(self):
        extensions = []
        if self.design_file_name:
            extensions.append(".yaml")
        if self.sql_file_name:
            extensions.append(".sql")
        if self.manifest_file_name:
            extensions.append(".manifest")
        if self._data_files:
            extensions.append("/csv/*")
        return "{}('{}{{{}}}')".format(self.__class__.__name__, self.source_path_name, ",".join(extensions))

    def bind_to_uri(self, scheme, netloc, path):
        self.scheme = scheme
        self.netloc = netloc
        self.path = path

    def uri(self, filename):
        """Return the full URI for the filename (either in S3 or local)."""
        if self.scheme == "s3":
            return "{0.scheme}://{0.netloc}/{1}".format(self, filename)
        elif self.scheme == "file":
            return filename
        else:
            raise ETLSystemError("illegal scheme in file set")

    def stat(self, filename):
        """Return file size (in bytes) and timestamp of last modification for this file."""
        if self.scheme == "s3":
            return etl.s3.object_stat(self.netloc, filename)
        elif self.scheme == "file":
            return os.path.getsize(filename), datetime.utcfromtimestamp(os.path.getmtime(filename)).isoformat(" ")
        else:
            raise ETLSystemError("illegal scheme in file set")

    @property
    def source_name(self):
        return self.target_table_name.schema

    @property
    def source_path_name(self):
        return "{}/{}-{}".format(
            self.target_table_name.schema, self.source_table_name.schema, self.source_table_name.table
        )

    def norm_path(self, filename: str) -> str:
        """
        Return "normalized" path based on filename of design file or SQL file.

        Assumption: If the filename ends with .yaml or .sql, then the file belongs under "schemas".
        Else the file belongs under "data".
        """
        if filename.endswith((".yaml", ".sql")):
            return "schemas/{}/{}".format(self.target_table_name.schema, os.path.basename(filename))
        else:
            return "data/{}/{}".format(self.target_table_name.schema, os.path.basename(filename))

    @property
    def data_files(self):
        return tuple(self._data_files)

    def add_data_file(self, filename):
        self._data_files.append(filename)

    @property
    def files(self):
        _files = []
        if self.design_file_name:
            _files.append(self.design_file_name)
        if self.sql_file_name:
            _files.append(self.sql_file_name)
        if self.manifest_file_name:
            _files.append(self.manifest_file_name)
        if self._data_files:
            _files.extend(self._data_files)
        return _files

    def __len__(self):
        return len(self.files)


def list_local_files(directory):
    """
    List all files in and anywhere below this directory.

    It is an error if the directory does not exist.
    Ignore swap files along the way.
    """
    normed_directory = os.path.normpath(directory)
    if not os.path.isdir(normed_directory):
        raise FileNotFoundError("Failed to find directory: '%s'" % normed_directory)
    logger.info("Looking for files locally in '%s'", normed_directory)
    for root, _, files in os.walk(os.path.normpath(normed_directory)):
        for filename in sorted(files):
            if not filename.endswith((".swp", "~", ".DS_Store")):
                yield os.path.join(root, filename)


def find_file_sets(uri_parts, selector, allow_empty=False):
    """
    Find files related to relations in S3 or locally.

    This is a generic method to collect files from either
    s3://bucket/prefix or file://localhost/directory or ./schemas
    based on the tuple describing a parsed URI. So that should be either
    ("s3", "bucket", "prefix", ...) or ("file", "localhost", "directory", ...)

    The selector (as a bare minimum) should have a reasonable set of base schemas.

    If :allow_empty is True and no files are found in the local filesystem, an empty list
    is returned. This is useful for commands supporting local development.
    """
    scheme, netloc, path = uri_parts[:3]
    if scheme == "s3":
        file_sets = _find_file_sets_from(
            etl.s3.list_objects_for_prefix(netloc, path + "/data", path + "/schemas"), selector
        )
        if not file_sets:
            raise FileNotFoundError("Found no matching files in 's3://{}/{}' for '{}'".format(netloc, path, selector))
    else:
        if os.path.exists(path):
            file_sets = _find_file_sets_from(list_local_files(path), selector)
            if not file_sets:
                if allow_empty:
                    file_sets = []
                else:
                    raise FileNotFoundError("Found no matching files in '{}' for '{}'".format(path, selector))
        elif allow_empty:
            logger.warning("Failed to find directory: '%s'", path)
            file_sets = []
        else:
            raise FileNotFoundError("Failed to find directory: '%s'" % path)
    # Bind the files that were found to where they were found
    for file_set in file_sets:
        file_set.bind_to_uri(scheme, netloc, path)
    return file_sets


def _find_file_sets_from(iterable, selector):
    """
    Return list of file sets ordered by (target) schema, (source) schema and (source) table.

    Remember that the (target) schema name is the same as the source name (for upstream sources).
    The selector's base schemas (if present) will override alphabetical sorting for the source_name.
    """
    target_map = {}

    # Always return files sorted by sources (in original order) and target name.
    schema_index = {name: index for index, name in enumerate(selector.base_schemas)}

    for file_info in _find_matching_files_from(iterable, selector):
        if file_info.file_type == "success":
            continue

        source_table_name = TableName(file_info.source_schema_name, file_info.table_name)
        target_table_name = TableName(file_info.schema_name, file_info.table_name)

        if target_table_name.identifier not in target_map:
            natural_order = schema_index.get(file_info.schema_name), source_table_name.identifier
            target_map[target_table_name.identifier] = RelationFileSet(
                source_table_name, target_table_name, natural_order
            )

        file_set = target_map[target_table_name.identifier]
        if file_info.file_type == "yaml":
            file_set.design_file_name = file_info.filename
        elif file_info.file_type == "sql":
            file_set.sql_file_name = file_info.filename
        elif file_info.file_type == "manifest":
            file_set.manifest_file_name = file_info.filename
        elif file_info.file_type == "data":
            file_set.add_data_file(file_info.filename)

    file_sets = sorted(target_map.values())
    logger.info("Found %d matching file(s) for %d table(s)", sum(len(fs) for fs in file_sets), len(file_sets))
    return file_sets


def find_data_files_in_s3(bucket_name: str, prefix: str) -> Iterator[str]:
    """Return paths of data files."""
    iterable = etl.s3.list_objects_for_prefix(bucket_name, prefix)
    for file_info in _find_matching_files_from(iterable, TableSelector()):
        if file_info.file_type == "data":
            yield file_info.filename


def delete_data_files_in_s3(bucket_name: str, prefix: str, selector: TableSelector, dry_run=False) -> None:
    """Delete all data files that match this prefix and selector pattern."""
    data_files = etl.s3.list_objects_for_prefix(bucket_name, prefix + "/data")
    deletable = [file_info.filename for file_info in _find_matching_files_from(data_files, selector)]
    if not deletable:
        logger.info("Found no matching files in 's3://%s/%s/data' to delete", bucket_name, prefix)
        return
    etl.s3.delete_objects(bucket_name, deletable, dry_run=dry_run)


def delete_schemas_files_in_s3(bucket_name: str, prefix: str, selector: TableSelector, dry_run=False) -> None:
    """Delete all table design and SQL files that match this prefix and selector pattern."""
    schemas_files = etl.s3.list_objects_for_prefix(bucket_name, prefix + "/schemas")
    deletable = [file_info.filename for file_info in _find_matching_files_from(schemas_files, selector)]
    if not deletable:
        logger.info("Found no matching files in 's3://%s/%s/schemas' to delete", bucket_name, prefix)
        return
    etl.s3.delete_objects(bucket_name, deletable, dry_run=dry_run)


def list_files(file_sets, long_format=False, sort_by_time=False) -> None:
    """
    List files in the given S3 bucket or from current directory.

    With the long format, shows content length and tallies up the total size in bytes.
    When sorted by time, only prints files but sorted by their timestamp.
    """
    if sort_by_time:
        found = []
        for file_set in file_sets:
            for filename in file_set.files:
                _, last_modified = file_set.stat(filename)
                found.append((last_modified, file_set.uri(filename)))
        print(
            etl.text.format_lines(
                [(uri, last_modified) for last_modified, uri in sorted(found)],
                header_row=["File", "Last modified"],
                max_column_width=120,
            )
        )
    else:
        total_length = 0
        for schema_name, file_group in groupby(file_sets, attrgetter("source_name")):
            print("Schema: '{}'".format(schema_name))
            for file_set in file_group:
                if file_set.manifest_file_name is None:
                    print("    Table: '{}'".format(file_set.target_table_name.identifier))
                else:
                    print(
                        "    Table: '{}' (with data from '{}')".format(
                            file_set.target_table_name.identifier, file_set.source_table_name.identifier
                        )
                    )
                for filename in file_set.files:
                    if long_format:
                        content_length, last_modified = file_set.stat(filename)
                        total_length += content_length
                        print("        {} ({:d}, {})".format(file_set.uri(filename), content_length, last_modified))
                    else:
                        print("        {}".format(file_set.uri(filename)))
        if total_length > 0:
            print("Total size in bytes: {:d} ({})".format(total_length, etl.text.approx_pretty_size(total_length)))
