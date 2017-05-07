class ETLError(Exception):
    """
    Parent to all ETL-oriented exceptions which allows to write effective except statements
    """


class ETLSystemError(ETLError):
    """
    Blunder in the ETL code -- you'll need to check the ETL code, sorry about that.

    This exception should be raised when rolling back ETL code might solve the issue.
    """


class ETLConfigError(ETLError):
    """
    Error in the data warehouse configuration -- you'll need to check your config or schemas.

    This exception should be raised when sync'ing a fixed version might solve the issue.
    """


class ETLRuntimeError(ETLError):
    """
    Error found at runtime -- you might be able to just try again or need to fix something upstream.

    This exception should be raised when the show-stopper lies outside of code or configuration.
    """


class ETLDelayedExit(ETLError):
    """
    Exception raised when errors were suppressed during "keep going" processing.

    The errors triggering this unseemly end could be either of the config or runtime variety.
    """


class SelfTestError(ETLSystemError):
    """
    Exception when one of the built-in test suites fails
    """


class SchemaInvalidError(ETLSystemError):
    pass


class SchemaValidationError(ETLConfigError):
    pass


class InvalidArgumentsError(ETLRuntimeError):
    """
    Exception when arguments are detected to be invalid by the command callback
    """


class InvalidEnvironmentError(ETLRuntimeError):
    """
    Exception when environment settings are invalid
    """


class MissingMappingError(ETLConfigError):
    """
    Exception when an attribute type's target type is unknown
    """


class TableDesignError(ETLConfigError):
    """
    Exception when a table design file is incorrect
    """


class TableDesignParseError(TableDesignError):
    """
    Exception when a table design file cannot be parsed
    """


class TableDesignSyntaxError(TableDesignError):
    """
    Exception when a table design file does not pass schema validation
    """


class TableDesignSemanticError(TableDesignError):
    """
    Exception when a table design file does not pass logic checks
    """


class TableDesignValidationError(TableDesignError):
    """
    Exception when a table design does not pass validation (against other table designs, upstream sources, ...
    """


class MissingQueryError(ETLConfigError):
    """
    Exception when the query (SQL file) is missing
    """


class CyclicDependencyError(ETLConfigError):
    """
    Exception when evaluation order runs in circles
    """


class UpstreamValidationError(ETLRuntimeError):
    """
    Exception when validation against upstream database fails
    """


class FailedConstraintError(ETLRuntimeError):
    def __init__(self, relation, constraint_type, columns, examples):
        self.relation = relation
        self.constraint_type = constraint_type
        self.columns = columns
        self.example_string = ', '.join(map(str, examples))

    def __str__(self):
        return ("Relation {0.relation} violates {0.constraint_type} constraint: "
                "Example duplicate values of {0.columns} are: {0.example_string}".format(self))


class DataExtractError(ETLRuntimeError):
    """
    Exception when extracting from an upstream source fails
    """


class UnknownTableSizeError(DataExtractError):
    pass


class SqoopExecutionError(DataExtractError):
    pass


class MissingCsvFilesError(DataExtractError):
    pass


class S3ServiceError(ETLRuntimeError):
    pass


class MissingManifestError(ETLRuntimeError):
    pass


class RequiredRelationLoadError(ETLRuntimeError):

    def __init__(self, failed, failed_and_required):
        implied_failures = ", ".join(identifier for identifier in failed_and_required if identifier != failed)
        if implied_failures:
            self.message = "Failure of {} implies failure of required relation(s): {}".format(failed, implied_failures)
        else:
            self.message = "Failure occurred for required {} relation".format(failed)

    def __str__(self):
        return self.message


class DataUnloadError(ETLRuntimeError):
    """
    Exception when the unload operation fails
    """
