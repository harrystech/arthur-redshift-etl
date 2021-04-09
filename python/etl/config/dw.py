"""Data warehouse configuration based on config files for setup, sources, transformations, users."""

from typing import Dict

import etl.config.env
import etl.db
import etl.names
import etl.templates
from etl.errors import ETLConfigError, InvalidEnvironmentError


class DataWarehouseUser:
    """
    Data warehouse users have always a name and group associated with them.

    Users may have a schema "belong" to them which they then have write access to.
    This is useful for system users, mostly, since end users should treat the
    data warehouse as read-only.
    """

    def __init__(self, user_info):
        self.name = user_info["name"]
        self.group = user_info["group"]
        self.schema = user_info.get("schema")


class S3DataFormat:
    """
    Enable specifying a data format (and options) for all source tables from one schema.

    The expected format in the configuration file is something like this:
        "s3_data_format": {
            "format": "JSON",
            "compression": "GZIP"
        }
    """

    def __init__(self, s3_data_format) -> None:
        self.format = s3_data_format.get("format")
        self.format_option = s3_data_format.get("format_option")
        self.compression = s3_data_format.get("compression")


class DataWarehouseSchema:
    """
    Define configuration of schema in the data warehouse, e.g. source vs. transformation..

    Schemas in the data warehouse fall into one of four buckets:
      (1) Upstream source backed by a database.  Data will be extracted from there and
        so we need to have a DSN with which we can connect.
      (2) Upstream source backed by CSV files in S3.  Data will be "extracted" in the sense
        that the ETL will create a manifest file suitable for the COPY command.  No DSN
        is needed here.
      (2.5) Target in S3 for "unload" command, which may also be an upstream source.
      (3) Schemas with CTAS or VIEWs that are computed during the ETL. Data cannot be extracted here
        (but maybe unload'ed).
      (4) Schemas reserved for users (where user could be a BI tool)

    Although there is a (logical) distinction between "sources" and "schemas" in the settings file
    those are really all the same here ...
    """

    def __init__(self, schema_info, etl_access=None):
        self.name = schema_info["name"]
        self.description = schema_info.get("description")
        # Schemas have an 'owner' user (with ALL privileges)
        # and lists of 'reader' and 'writer' groups with corresponding permissions
        self.owner = schema_info["owner"]
        self.reader_groups = schema_info.get("readers", schema_info.get("groups", []))
        self.writer_groups = schema_info.get("writers", [])
        # Booleans to help figure out which bucket the schema is in (see doc for class)
        self.is_database_source = "read_access" in schema_info
        self.is_static_source = "s3_bucket" in schema_info and "s3_path_template" in schema_info
        self.is_upstream_source = self.is_database_source or self.is_static_source
        self.has_transformations = not self.is_upstream_source
        self.is_an_unload_target = "s3_bucket" in schema_info and "s3_unload_path_template" in schema_info
        # How to access the source of the schema (per DSN (of source or DW)? per S3?)
        if self.is_database_source:
            self._dsn_env_var = schema_info["read_access"]
        elif self.is_static_source or self.is_an_unload_target:
            self._dsn_env_var = None
        else:
            self._dsn_env_var = etl_access
        self.has_dsn = self._dsn_env_var is not None
        # The S3 bucket, path template and unload path template must be rendered since they may
        # be template strings.
        self._s3_bucket_template = schema_info.get("s3_bucket")
        self._s3_path_template = schema_info.get("s3_path_template")
        self._s3_unload_path_template = schema_info.get("s3_unload_path_template")
        # Additional attributes used for specifying data formats for files in S3
        self.s3_data_format = S3DataFormat(schema_info.get("s3_data_format", {}))
        # When dealing with this schema of some upstream source, which tables should be used
        # and which should be skipped?
        self.include_tables = schema_info.get("include_tables", [self.name + ".*"])
        self.exclude_tables = schema_info.get("exclude_tables", [])

    @property
    def s3_bucket(self) -> str:
        """Render S3 Bucket name (if it references Arthur configuration, e.g., the data lake)."""
        return etl.templates.render_from_config(
            self._s3_bucket_template, context="s3_bucket of schema '{}'".format(self.name)
        )

    @property
    def s3_path_prefix(self) -> str:
        """Render S3 path prefix in particular wrt. prefix (environment) and dates."""
        return etl.templates.render_from_config(
            self._s3_path_template, context="s3_path_template of schema '{}'".format(self.name)
        )

    @property
    def s3_unload_path_prefix(self) -> str:
        """Render S3 unload path prefix in particular wrt. prefix (environment) and dates."""
        return etl.templates.render_from_config(
            self._s3_unload_path_template, context="s3_unload_path_template of schema '{}'".format(self.name)
        )

    @property
    def dsn(self):
        """
        Return connection string to find this schema upstream or in the data warehouse.

        This returns connection string suitable for the schema which is
        - the value of the environment variable named in the read_access field for upstream sources
        - the value of the environment variable named in the etl_access field of the data warehouse
          for schemas that have CTAS or views (transformations)
        Evaluation of the DSN (and the environment variable) is deferred so that an environment
        variable may be not set if it is actually not used.
        """
        # Note this returns None for a static source.
        if self._dsn_env_var:
            return etl.db.parse_connection_string(etl.config.env.get(self._dsn_env_var))

    @property
    def groups(self):
        return self.reader_groups + self.writer_groups

    @property
    def backup_name(self):
        return etl.names.as_backup_name(self.name)

    @property
    def staging_name(self):
        return etl.names.as_staging_name(self.name)


class DataWarehouseConfig:
    """Pretty interface to create objects from the settings files."""

    def __init__(self, settings):
        dw_settings = settings["data_warehouse"]
        schema_settings = settings.get("sources", []) + dw_settings.get("transformations", [])

        # Environment variables with DSN
        self._admin_access = dw_settings["admin_access"]
        self._etl_access = dw_settings["etl_access"]
        self._check_access_to_cluster()
        root = DataWarehouseUser(dw_settings["owner"])
        # Users are in the order from the config
        other_users = [DataWarehouseUser(user) for user in dw_settings.get("users", []) if user["name"] != "default"]

        # Note that the "owner," which is our super-user of sorts, comes first.
        self.users = [root] + other_users
        schema_owner_map = {u.schema: u.name for u in self.users if u.schema}

        # Schemas (upstream sources followed by transformations, keeps order of settings file)
        self.schemas = [
            DataWarehouseSchema(dict(info, owner=schema_owner_map.get(info["name"], root.name)), self._etl_access)
            for info in schema_settings
            if not info.get("external", False)
        ]
        self._schema_lookup = {schema.name: schema for schema in self.schemas}

        # Schemas may grant access to groups that have no bootstrapped users, so create all
        # mentioned user groups.
        other_groups = {u.group for u in other_users} | {g for schema in self.schemas for g in schema.reader_groups}

        # Groups are in sorted order after the root group
        self.groups = [root.group] + sorted(other_groups)
        try:
            [self.default_group] = [user["group"] for user in dw_settings["users"] if user["name"] == "default"]
        except ValueError:
            raise ETLConfigError("Failed to find group of default user")
        # Relation glob patterns indicating unacceptable load failures; matches everything if unset
        required_patterns = dw_settings.get("required_for_success", [])
        self.required_in_full_load_selector = etl.names.TableSelector(required_patterns)

        # Map of SQL types to be able to automatically insert "expressions" into table design files.
        self.type_maps = settings["type_maps"]

        # External schemas are un-managed by Arthur and require late-binding views.
        self.external_schema_names = [info["name"] for info in schema_settings if info.get("external", False)]

    def _check_access_to_cluster(self):
        """
        Make sure that ETL user and admin may connect and connect to different databases.

        This makes sure that the ETL and Admin access point to the same cluster (identified by host
        and port), but they must point to different databases in the cluster.
        It is ok if an environment variable is missing.  But if both are present they must align.
        """
        try:
            etl_dsn = self.dsn_etl
            admin_dsn = self.dsn_admin
            if etl_dsn["host"] != admin_dsn["host"]:
                raise InvalidEnvironmentError("Host is different between ETL and admin user")
            if etl_dsn.get("port") != admin_dsn.get("port"):
                raise InvalidEnvironmentError("Port is different between ETL and admin user")
            if etl_dsn["database"] == admin_dsn["database"]:
                raise InvalidEnvironmentError("Database is not different between ETL and admin user")
        except (KeyError, ValueError):
            pass

    @property
    def dsn_admin(self) -> Dict[str, str]:
        return etl.db.parse_connection_string(etl.config.env.get(self._admin_access))

    @property
    def dsn_etl(self) -> Dict[str, str]:
        return etl.db.parse_connection_string(etl.config.env.get(self._etl_access))

    @property
    def dsn_admin_on_etl_db(self) -> Dict[str, str]:
        # To connect as a superuser, but on the same database on which you would ETL
        return dict(self.dsn_admin, database=self.dsn_etl["database"])

    @property
    def owner(self) -> DataWarehouseUser:
        return self.users[0]

    def schema_lookup(self, schema_name) -> DataWarehouseSchema:
        return self._schema_lookup[schema_name]
