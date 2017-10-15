class ClusterConfig:
    """
    Configuration of your ES service domain. Override in config.py!
    """
    endpoint = None
    region = None
    # index_template = "test-dw-etl-logs-%Y-%m-%d-%H-%M"
    index_template = "dw-etl-logs-%Y-%m-%d"
    doc_type = "arthur-redshift-etl-log"
