class ETLError(Exception):
    """Parent to all ETL-oriented exceptions which allows to write effective except statements"""
    pass


class ETLSystemError(ETLError):
    """
    Blunder in the ETL code -- you'll need to check the ETL code, sorry about that.

    This exception should be raised when rolling back ETL code might solve the issue.
    """
    pass


class ETLConfigError(ETLError):
    """
    Error in the data warehouse configuration -- you'll need to check your config or schemas.

    This exception should be raised when sync'ing a fixed version might solve the issue.
    """
    pass


class ETLRuntimeError(ETLError):
    """
    Error found at runtime -- you might be able to just try again or need to fix something upstream.

    This exception should be raised when the show-stopper lies outside of code or configuration.
    """


class MissingMappingError(ETLConfigError):
    """Exception when an attribute type's target type is unknown"""
    pass


class TableDesignError(ETLConfigError):
    """Exception when a table design file is incorrect"""
    pass


class TableDesignParseError(TableDesignError):
    """Exception when a table design file cannot be parsed"""
    pass


class TableDesignValidationError(TableDesignError):
    """Exception when a table design file does not pass schema validation"""
    pass


class TableDesignSemanticError(TableDesignError):
    """Exception when a table design file does not pass logic checks"""
    pass


class MissingQueryError(ETLConfigError):
    """Exception when the query (SQL file) is missing"""
    pass


class CyclicDependencyError(ETLConfigError):
    """Exception when evaluation order runs in circles"""
    pass


class ReloadConsistencyError(ETLConfigError):
    """Exception when unloaded and re-loaded columns don't match"""
    pass


class UpstreamValidationError(ETLRuntimeError):
    """Exception when validation against upstream database fails"""
    pass


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
    """Exception when extracting from an upstream source fails"""
    pass


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


class RequiredRelationFailed(ETLRuntimeError):
    def __init__(self, failed_description, illegal_failures, required_selector):
        self.failed_description = failed_description
        self.illegal_failures = illegal_failures
        self.required_selector = required_selector

    def __str__(self):
        return "Failure of {d} implies failures of {f}, which are required by selector {r}".format(
            d=self.failed_description.identifier, f=', '.join(self.illegal_failures), r=self.required_selector)


class DataUnloadError(ETLRuntimeError):
    """Exception when the unload operation fails"""
    pass
