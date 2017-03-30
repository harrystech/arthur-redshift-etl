import etl

# TODO Think about a better hierarchy of errors (runtime vs. configuration vs. ...)


class MissingMappingError(etl.ETLError):
    """Exception when an attribute type's target type is unknown"""
    pass


class TableDesignError(etl.ETLError):
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


class MissingQueryError(etl.ETLError):
    """Exception when the query (SQL file) is missing"""
    pass


class CyclicDependencyError(etl.ETLError):
    pass


class ReloadConsistencyError(etl.ETLError):
    """Exception when unloaded and re-loaded columns don't match"""
    pass


class UniqueConstraintError(etl.ETLError):
    def __init__(self, relation, constraint_type, columns, examples):
        self.relation = relation
        self.constraint_type = constraint_type
        self.columns = columns
        self.example_string = ', '.join(map(str, examples))

    def __str__(self):
        return ("Relation {0.relation} violates {0.constraint_type} constraint: "
                "Example duplicate values of {0.columns} are: {0.example_string}".format(self))


class DataExtractError(etl.ETLError):
    """Exception when extracting from an upstream source fails"""
    pass


class UnknownTableSizeError(DataExtractError):
    pass


class SqoopExecutionError(DataExtractError):
    pass


class MissingCsvFilesError(DataExtractError):
    pass


# FIXME No longer used?
# class BadSourceDefinitionError(etl.ETLError):
#     pass


class S3ServiceError(etl.ETLError):
    pass


class MissingManifestError(etl.ETLError):
    pass


class RequiredRelationFailed(etl.ETLError):
    def __init__(self, failed_description, illegal_failures, required_selector):
        self.failed_description = failed_description
        self.illegal_failures = illegal_failures
        self.required_selector = required_selector

    def __str__(self):
        return "Failure of {d} implies failures of {f}, which are required by selector {r}".format(
            d=self.failed_description.identifier, f=', '.join(self.illegal_failures), r=self.required_selector)


class DataUnloadError(etl.ETLError):
    pass


class UnloadTargetNotFoundError(DataUnloadError):
    pass
