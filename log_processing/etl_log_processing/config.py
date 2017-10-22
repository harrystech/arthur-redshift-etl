"""
Access to shared settings and managing indices
"""

import sys
import time
import datetime

import boto3
import elasticsearch
import requests_aws4auth

from etl_log_processing import parse

# Index for our log records
LOG_INDEX_PATTERN = "dw-etl-logs-*"
LOG_DOC_TYPE = "arthur-redshift-etl-log"

ES_ENDPOINT_BY_ENV_TYPE = "/DW-ETL/ES-By-Env-Type/{env_type}"
ES_ENDPOINT_BY_BUCKET = "/DW-ETL/ES-By-Bucket/{bucket_name}"


def log_index(date=None):
    # Smallest supported granularity is a day
    if date is None:
        instant = datetime.date.today()
    else:
        instant = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    return instant.strftime(LOG_INDEX_PATTERN.replace("-*", "-%Y-%W"))


def set_es_endpoint(env_type, bucket_name, endpoint):
    print("Setting endpoint")
    client = boto3.client('ssm')
    for parameter in (ES_ENDPOINT_BY_ENV_TYPE, ES_ENDPOINT_BY_BUCKET):
        name = parameter.format(env_type=env_type, bucket_name=bucket_name)
        client.put_parameter(
            Name=name,
            Description="Value of 'host:port' of Elasticsearch cluster for log processing",
            Value=endpoint,
            Type="String",
            Overwrite=True
        )
        client.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId=name,
            Tags=[
                {
                    "Key": "user:project",
                    "Value": "data-warehouse"
                }
            ]
        )


def get_es_endpoint(env_type=None, bucket_name=None):
    if env_type is not None:
        name = ES_ENDPOINT_BY_ENV_TYPE.format(env_type=env_type)
    elif bucket_name is not None:
        name = ES_ENDPOINT_BY_BUCKET.format(bucket_name=bucket_name)
    else:
        raise ValueError("one of 'env_type' or 'bucket_name' must be not None")
    client = boto3.client('ssm')
    print("Looking up parameter '{}'".format(name))
    response = client.get_parameter(Name=name, WithDecryption=False)
    es_endpoint = response["Parameter"]["Value"]
    host, port = es_endpoint.rsplit(':', 1)
    return host, int(port)


def _aws_auth():
    # https://github.com/sam-washington/requests-aws4auth/pull/2
    session = boto3.Session()
    print("Retrieving credentials (profile_name={}, region_name={})".format(session.profile_name, session.region_name),
          file=sys.stderr)
    credentials = session.get_credentials()
    aws4auth = requests_aws4auth.AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, "es",
                                          session_token=credentials.token)

    def wrapped_aws4auth(request):
        return aws4auth(request)

    return wrapped_aws4auth


def connect_to_es(host, port, use_auth=False):
    """
    Return client. Unless running from authorized IP, set use_auth to True so that credentials are based on role.
    """
    if use_auth:
        http_auth = _aws_auth()
    else:
        http_auth = None
    es = elasticsearch.Elasticsearch(
        hosts=[{"host": host, "port": port}],
        use_ssl=True,
        verify_certs=True,
        connection_class=elasticsearch.connection.RequestsHttpConnection,
        http_auth=http_auth,
        send_get_body_as="POST"
    )
    return es


def put_index_template(client):
    version = int(time.time())
    template_id = LOG_INDEX_PATTERN.replace("-*", "-template")
    body = {
        "template": LOG_INDEX_PATTERN,
        "version": version,
        "settings": {
            "index.mapper.dynamic": False
        },
        "mappings": {
            LOG_DOC_TYPE: parse.LogParser.index_fields()
        }
    }
    print("Updating index template '{}' (doc_type={}, version={})".format(template_id, LOG_DOC_TYPE, version))
    client.indices.put_template(template_id, body)


def get_indices(client):
    print("Looking for indices matching {}".format(LOG_INDEX_PATTERN))
    response = client.indices.get(index=LOG_INDEX_PATTERN, allow_no_indices=True)
    for index in sorted(response):
        print(response[index]['settings']['index'])


def main():
    if len(sys.argv) != 4:
        print("Usage: {} env_type bucket_name endpoint")
        exit(1)
    prg_name, env_type, bucket_name, endpoint = sys.argv

    # TODO Split these out into subcommands!

    set_es_endpoint(env_type, bucket_name, endpoint)
    host, port = get_es_endpoint(env_type=env_type)
    print("Found ES domain at '{}:{}'".format(host, port))

    es = connect_to_es(host, port, use_auth=False)
    put_index_template(es)
    get_indices(es)

    # TODO Delete old indices


if __name__ == "__main__":
    main()
