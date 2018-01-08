import time
from functools import partial


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


class TransientETLError(ETLRuntimeError):
    """
    Represents all types of runtime errors that should be retryable due to the cause being something in the external
    environment that might change over time.
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


class DataExtractError(TransientETLError):
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


class RetriesExhaustedError(ETLRuntimeError):
    """
    Raised when all retry attempts have been exhausted.

    The causing exception should be passed to this one to complete the chain of failure accountability. For example:
    >>> raise RetriesExhaustedError from ETLRuntimeError("Boom!")
    Traceback (most recent call last):
        ...
    etl.errors.RetriesExhaustedError
    """


def retry(max_retries: int, func: partial, logger):
    """
    Retry a function a maximum number of times and return its results.
    The function should be a functools.partial called with no arguments.
    Sleeps for 5 ^ attempt_number seconds if there are remaining retry attempts.

    The given func function is only retried if it throws a TransientETLError. Any other error is considered
    permanent, and therefore no retry attempt is made.
    """
    failure_reason = None
    successful_result = None

    for attempt in range(max_retries + 1):
        try:
            successful_result = func()
        except TransientETLError as e:
            # Only retry transient errors
            failure_reason = e
            remaining_attempts = max_retries - attempt
            if remaining_attempts:
                sleep_time = 5 ** (attempt + 1)
                logger.warning("Encountered the following error (retrying %s more times after %s second sleep): %s",
                               remaining_attempts, sleep_time, str(e))
                time.sleep(sleep_time)
            continue
        except:
            # We consider all other errors permanent and immediately re-raise without retrying
            raise
        else:
            break
    else:
        raise RetriesExhaustedError("reached max number of retries (={:d})".format(max_retries)) from failure_reason

    return successful_result
