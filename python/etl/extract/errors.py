import etl


class UnknownTableSizeError(etl.ETLError):
    pass


class DataExtractError(etl.ETLError):
    pass


class SqoopExecutionError(DataExtractError):
    pass


class MissingCsvFilesError(DataExtractError):
    pass
