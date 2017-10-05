class ClusterConfig:
    """
    Configuration of your ES service domain. Override in config.py!
    """
    endpoint = None
    region = None
    index_template = "dw-etl-logs-%Y-%m-%d"
    doc_type = "arthur-redshift-etl-log"
