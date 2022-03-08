from etl.text import join_with_single_quotes


class ETLError(Exception):
    """Parent to all ETL-oriented exceptions which allows to write effective except statements."""


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


class TransientETLError(ETLRuntimeError):
    """
    Error found at runtime which we might overcome by retrying the operation.

    Represents all types of runtime errors that should be retryable due to the cause being
    something in the external environment that might change over time.
    """


class ETLDelayedExit(ETLError):
    """
    Exception raised when errors were suppressed during "keep going" processing.

    The errors triggering this unseemly end could be either of the config or runtime variety.
    """


class SelfTestError(ETLSystemError):
    """Exception when one of the built-in test suites fails."""


class SchemaInvalidError(ETLSystemError):
    pass


class SchemaValidationError(ETLConfigError):
    pass


class MissingValueTemplateError(ETLSystemError):
    """Exception when a template cannot be rendered because a configuration setting is missing."""


class InvalidArgumentError(ETLRuntimeError):
    """Exception when arguments are detected to be invalid by the command callback."""


class InvalidEnvironmentError(ETLRuntimeError):
    """Exception when environment settings are invalid."""


class TableDesignError(ETLConfigError):
    """Exception when a table design file is incorrect."""


class TableDesignParseError(TableDesignError):
    """Exception when a table design file cannot be parsed."""


class TableDesignSyntaxError(TableDesignError):
    """Exception when a table design file does not pass schema validation."""


class TableDesignSemanticError(TableDesignError):
    """Exception when a table design file does not pass logic checks."""


class TableDesignValidationError(TableDesignError):
    """
    Exception when a table design does not pass validation.

    Reasons to fail validation includes issues against upstream sources or other table designs.
    """


class MissingQueryError(ETLConfigError):
    """Exception when the query (SQL file) is missing."""


class CyclicDependencyError(ETLConfigError):
    """Exception when evaluation order runs in circles."""


class UpstreamValidationError(ETLRuntimeError):
    """Exception when validation against upstream database fails."""


class DataExtractError(TransientETLError):
    """Exception when extracting from an upstream source fails."""


class UnknownTableSizeError(DataExtractError):
    pass


class SqoopExecutionError(DataExtractError):
    pass


class MissingCsvFilesError(DataExtractError):
    pass


class S3ServiceError(ETLRuntimeError):
    """Exception when we encounter problems with S3."""


class RelationConstructionError(ETLRuntimeError):
    """Exception when we fail to drop or create a relation."""


class RelationDataError(ETLRuntimeError):
    """Parent of exceptions that occur while trying to load or loading a relation."""


class MissingManifestError(RelationDataError):
    pass


class UpdateTableError(RelationDataError):
    pass


class MissingExtractEventError(RelationDataError):
    def __init__(self, source_relations, extracted_targets):
        missing_relations = [
            relation
            for relation in source_relations
            if relation.identifier not in extracted_targets
        ]
        self.message = (
            "Some source relations did not have extract events after the step start time: "
            + join_with_single_quotes(missing_relations)
        )

    def __str__(self):
        return self.message


class FailedConstraintError(RelationDataError):
    def __init__(self, relation, constraint_type, columns, examples):
        self.identifier = relation.identifier
        self.constraint_type = constraint_type
        self.columns = columns
        self.example_string = ",\n  ".join(map(str, examples))
        self.message = (
            f"relation '{self.identifier}' violates {self.constraint_type} constraint.\n"
            f"Example duplicate values of {self.columns} are:\n  {self.example_string}"
        )

    def __str__(self):
        return self.message


class RequiredRelationLoadError(ETLRuntimeError):
    def __init__(self, failed_relations, bad_apple=None):
        self.message = (
            f"required relation(s) with failure: {join_with_single_quotes(failed_relations)}"
        )
        if bad_apple:
            self.message += f", triggered by load failure of '{bad_apple}'"

    def __str__(self):
        return self.message


class DataUnloadError(ETLRuntimeError):
    """Exception when the unload operation fails."""


class RetriesExhaustedError(ETLRuntimeError):
    """
    Raised when all retry attempts have been exhausted.

    The causing exception should be passed to this one to complete the chain of failure
    accountability. For example:
    >>> raise RetriesExhaustedError from ETLRuntimeError("Boom!")
    Traceback (most recent call last):
        ...
    etl.errors.RetriesExhaustedError
    """
