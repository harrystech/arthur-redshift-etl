class ClusterConfig:
    """
    Configuration of your ES service domain. Override in config.py!
    """
    endpoint = None
    region = "us-east-1"
    index = "etl-redshift-logs"
    doc_type = "arthur-log"
