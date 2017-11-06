"""
Utilities to interact with Amazon S3 and occasionally the local file system.

The basic location of a file depends on the data source and can be one of:
.../schemas/{source_or_schema_name}/{source_schema_name}-{table_name}.yaml -- for table design files
.../schemas/{schema_name}/{source_schema_name}-{table_name}.sql -- for queries for CTAS or views
.../data/{source_name}/{source_schema_name}-{table_name}.manifest -- for a manifest of data files
.../data/{source_name}/{source_schema_name}-{table_name}/csv/part-*.gz -- for the data files themselves.

If the files are in S3, then the start of the path is always s3://{bucket_name}/{prefix}/...

If the files are stored locally, then the start of the path is probably simply the current directory ('.').

For tables that are backed by upstream sources, the directory after 'schemas' or 'data' will be the
name of the source in the configuration file.

For CTAS or views, the directory after 'schemas' is the name of the schema in the data warehouse
configuration.  The 'source_schema_name' is only used for sorting.
"""

import logging
import os
import os.path
import re
from datetime import datetime
from itertools import groupby
from operator import attrgetter

import etl.config
import etl.s3
import etl.text
from etl.errors import ETLSystemError
from etl.names import TableName, TableSelector

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class TableFileSet:
    """
    Class to hold design file, SQL file and data files (CSV and manifest) belonging to a table.

    TableFileSets have a :natural_order based on their schema's position in the DataWarehouseConfig's
    schema list and their :source_table_name.  (So we try to sort sources by their order
    in the configuration and not alphabetically.)

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
        return "{}('{}{{{}}}')".format(self.__class__.__name__, self.source_path_name, ','.join(extensions))

    def bind_to_uri(self, scheme, netloc, path):
        self.scheme = scheme
        self.netloc = netloc
        self.path = path

    def uri(self, filename):
        """
        Return the full URI for the filename, which probably should be one of the files from this set
        """
        if self.scheme == "s3":
            return "{0.scheme}://{0.netloc}/{1}".format(self, filename)
        elif self.scheme == "file":
            return filename
        else:
            raise ETLSystemError("illegal scheme in file set")

    def stat(self, filename):
        """
        Return file size (in bytes) and timestamp of last modification for the file which should be one from this set.
        """
        if self.scheme == "s3":
            return etl.s3.object_stat(self.netloc, filename)
        elif self.scheme == "file":
            return local_file_stat(filename)
        else:
            raise ETLSystemError("illegal scheme in file set")

    @property
    def source_name(self):
        return self.target_table_name.schema

    @property
    def source_path_name(self):
        return "{}/{}-{}".format(self.target_table_name.schema,
                                 self.source_table_name.schema,
                                 self.source_table_name.table)

    @property
    def csv_path_name(self):
        return os.path.join("data", self.source_path_name, "csv")

    def norm_path(self, filename: str) -> str:
        """
        Return "normalized" path based on filename of design file or SQL file.

        Assumption: If the filename ends with .yaml or .sql, then the file belongs under "schemas".
        Else the file belongs under "data".
        """
        if filename.endswith((".yaml", ".yml", ".sql")):
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


def local_file_stat(filename):
    """
    Return size in bytes and last modification timestamp from local file.
    """
    return os.path.getsize(filename), datetime.utcfromtimestamp(os.path.getmtime(filename)).isoformat(' ')


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
    for root, dirs, files in os.walk(os.path.normpath(normed_directory)):
        for filename in sorted(files):
            if not filename.endswith(('.swp', '~', '.DS_Store')):
                yield os.path.join(root, filename)


def find_file_sets(uri_parts, selector, allow_empty=False):
    """
    Generic method to collect files from either s3://bucket/prefix or file://localhost/directory
    based on the tuple describing a parsed URI, which should be either
    ("s3", "bucket", "prefix", ...) or ("file", "localhost", "directory", ...)
    The selector (as a bare minimum) should have a reasonable set of base schemas.

    If :allow_empty is True and no files are found in the local filesystem, an empty list is returned.
    """
    scheme, netloc, path = uri_parts[:3]
    if scheme == "s3":
        file_sets = _find_file_sets_from(etl.s3.list_objects_for_prefix(netloc, path + '/data', path + '/schemas'),
                                         selector)
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


def _find_matching_files_from(iterable, pattern, return_success_file=False):
    """
    Match file names against the target pattern and expected path format,
    return file information based on source name, source schema, table name and file type.

    This generator provides all files that are possibly relevant and it
    is up to the consumer to ensure consistency (e.g. that a design
    file exists or that a SQL file is not present along with a manifest).

    Files ending in '_SUCCESS' or '_$folder$' are ignored (which are created by some Spark jobs).
    """
    file_names_re = re.compile(r"""(?:^schemas|/schemas|^data|/data)
                                   /(?P<source_name>\w+)
                                   /(?P<schema_name>\w+)-(?P<table_name>\w+)
                                   (?:(?P<file_ext>.yaml|.sql|.manifest|/csv/(:?part-.*(:?\.gz)?|_SUCCESS)))$
                               """, re.VERBOSE)

    for filename in iterable:
        match = file_names_re.search(filename)
        if match:
            values = match.groupdict()
            target_table_name = TableName(values['source_name'], values['table_name'])
            if pattern.match(target_table_name):
                file_ext = values["file_ext"]
                if file_ext in [".yaml", ".sql", ".manifest"]:
                    values["file_type"] = file_ext[1:]
                elif file_ext.endswith("_SUCCESS"):
                    values["file_type"] = "success"
                elif file_ext.startswith("/csv"):
                    values["file_type"] = "data"
                # E.g. when deleting files out of a folder we want to know about the /csv/_SUCCESS file.
                if return_success_file or values["file_type"] != "success":
                    yield (filename, values)
        elif not filename.endswith("_$folder$"):
            logger.warning("Found file not matching expected format: '%s'", filename)


def _find_file_sets_from(iterable, selector):
    """
    Return list of file sets ordered by (target) schema name, (source) schema_name and (source) table_name.
    Remember that the (target) schema name is the same as the source name (for upstream sources).
    The selector's base schemas (if present) will override alphabetical sorting for the source_name.
    """
    target_map = {}

    # Always return files sorted by sources (in original order) and target name.
    schema_index = {name: index for index, name in enumerate(selector.base_schemas)}

    for filename, values in _find_matching_files_from(iterable, selector):
        source_table_name = TableName(values["schema_name"], values["table_name"])
        target_table_name = TableName(values["source_name"], values["table_name"])

        if target_table_name.identifier not in target_map:
            natural_order = schema_index.get(values["source_name"]), source_table_name.identifier
            target_map[target_table_name.identifier] = TableFileSet(source_table_name, target_table_name, natural_order)

        file_set = target_map[target_table_name.identifier]
        file_type = values['file_type']
        if file_type == 'yaml':
            file_set.design_file_name = filename
        elif file_type == 'sql':
            file_set.sql_file_name = filename
        elif file_type == 'manifest':
            file_set.manifest_file_name = filename
        elif file_type == 'data':
            file_set.add_data_file(filename)

    file_sets = sorted(target_map.values())
    logger.info("Found %d matching file(s) for %d table(s)", sum(len(fs) for fs in file_sets), len(file_sets))
    return file_sets


def delete_files_in_bucket(bucket_name: str, prefix: str, selector: TableSelector, dry_run: bool=False) -> None:
    """
    Delete all files that might be relevant given the choice of schemas and the target selection.
    """
    iterable = etl.s3.list_objects_for_prefix(bucket_name, prefix + '/data', prefix + '/schemas')
    deletable = [filename for filename, v in _find_matching_files_from(iterable, selector, return_success_file=True)]
    if dry_run:
        for key in deletable:
            logger.info("Dry-run: Skipping deletion of 's3://%s/%s'", bucket_name, key)
    else:
        if deletable:
            etl.s3.delete_objects(bucket_name, deletable)
        else:
            logger.info("Found no matching files in 's3://%s/%s' to delete", bucket_name, prefix)


def approx_pretty_size(total_bytes) -> str:
    """
    Return a humane and pretty size approximation.

    This looks silly bellow 1KB but I'm OK with that.
    Don't call this with negative total_bytes or your pet hamster will go bald.

    >>> approx_pretty_size(50)
    '1KB'
    >>> approx_pretty_size(2000)
    '2KB'
    >>> approx_pretty_size(2048)
    '2KB'
    >>> approx_pretty_size(3000000)
    '3MB'
    >>> approx_pretty_size(4000000000)
    '4GB'
    >>> approx_pretty_size(-314)
    Traceback (most recent call last):
        ...
    ValueError: total_bytes may not be negative
    """
    if total_bytes < 0:
        raise ValueError("total_bytes may not be negative")
    for scale, unit in ((1024 * 1024 * 1024, 'GB'), (1024 * 1024, 'MB'), (1024, 'KB')):
        div, rem = divmod(total_bytes, scale)
        if div > 0:
            if rem > 0:
                div += 1  # always round up
            break
    else:
        div, unit = 1, 'KB'
    return "{:d}{}".format(div, unit)


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
        print(etl.text.format_lines([(uri, last_modified) for last_modified, uri in sorted(found)],
                                    header_row=["File", "Last modified"], max_column_width=120))
    else:
        total_length = 0
        for schema_name, file_group in groupby(file_sets, attrgetter("source_name")):
            print("Schema: '{}'".format(schema_name))
            for file_set in file_group:
                if file_set.manifest_file_name is None:
                    print("    Table: '{}'".format(file_set.target_table_name.identifier))
                else:
                    print("    Table: '{}' (with data from '{}')".format(file_set.target_table_name.identifier,
                                                                         file_set.source_table_name.identifier))
                for filename in file_set.files:
                    if long_format:
                        content_length, last_modified = file_set.stat(filename)
                        total_length += content_length
                        print("        {} ({:d}, {})".format(file_set.uri(filename), content_length, last_modified))
                    else:
                        print("        {}".format(file_set.uri(filename)))
        if total_length > 0:
            print("Total size in bytes: {:d} ({})".format(total_length, approx_pretty_size(total_length)))
