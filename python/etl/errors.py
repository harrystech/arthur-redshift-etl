class ETLError(Exception):
    """
    Parent to all ETL-oriented exceptions which allows to write effective except statements
    """


class PermanentETLError(ETLError):
    """
    Represents all types of errors that are known to be permanent failures due irrecoverable situations within the ETL
    """


class TransientETLError(ETLError):
    """
    Represents all types of errors that could potentially resolve themselves due to changes in the outside environment
    or some kind of intervention, and are thus retryable.
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


class MissingValueTemplateError(ETLSystemError):
    """
    Exception when a template cannot be rendered because a configuration setting is missing.
    """


class InvalidArgumentError(ETLRuntimeError):
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


class DataExtractError(ETLRuntimeError):
    """
    Exception when extracting from an upstream source fails
    """


class UnknownTableSizeError(DataExtractError):
    pass


class SqoopExecutionError(DataExtractError):
    pass


class TransientSqoopExecutionError(TransientETLError):
    # TODO: Refactor exception hierarchy to properly tag all transient errors
    pass


class MissingCsvFilesError(DataExtractError):
    pass


class S3ServiceError(ETLRuntimeError):
    """
    Exception when we encounter problems with S3
    """


class RelationConstructionError(ETLRuntimeError):
    """
    Exception when we fail to drop or create a relation
    """


class RelationDataError(ETLRuntimeError):
    """
    Exception when we have problems due to data that was supposed to go into the relation or that landed there.
    """


class MissingManifestError(RelationDataError):
    pass


class UpdateTableError(RelationDataError):
    pass


class FailedConstraintError(RelationDataError):

    def __init__(self, relation, constraint_type, columns, examples):
        self.identifier = relation.identifier
        self.constraint_type = constraint_type
        self.columns = columns
        self.example_string = ',\n  '.join(map(str, examples))

    def __str__(self):
        return ("relation {0.identifier} violates {0.constraint_type} constraint.\n"
                "Example duplicate values of {0.columns} are:\n  {0.example_string}".format(self))


class RequiredRelationLoadError(ETLRuntimeError):

    def __init__(self, failed_relations, bad_apple=None):
        # Avoiding `join_with_quotes` here to keep this module import-free
        self.message = "required relation(s) with failure: "
        self.message += ", ".join("'{}'".format(name) for name in failed_relations)
        if bad_apple:
            self.message += ", triggered by load failure of '{}'".format(bad_apple)

    def __str__(self):
        return self.message


class DataUnloadError(ETLRuntimeError):
    """
    Exception when the unload operation fails
    """


class RetriesExhaustedError(PermanentETLError):
    """
    Raised when all retry attempts have been exhausted.

    The causing exception should be passed to this one to complete the chain of failure accountability. For example:
    >>> raise RetriesExhaustedError from ETLRuntimeError("Boom!")
    """
