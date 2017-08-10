# Overview

The goal of these tools is to make the logs from ETLs available in Elasticsearch
for post-processing, searches, graphs, etc.

## Setup and Requirements

### Amazon Elasticsearch Service Dommains

You have to have an Amazon ES domain running. Add the endpoint to `config.py`, see the documentation there.

For more information about Elasticsearch in AWS, see [Getting Started Guide](http://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-gsg.html).


### Lambda Permissions

To use the lambda function to automatically upload to ES any log files to show up in S3,
you will have to create a new role to use with the lambda.

The role must have these permissions:
* `AWSLambdaBasicExecutionRole`
* Read permissions for your S3 bucket
* Write permissions for your ES domain

TODO Create role automatically, `temp-log-parser`, add to config


### Python virtual environment

This code uses Python 3. See the [toplevel README](../README.md) for installation instructions.

In order to run this code locally or to upload it as a lambda function, you have to have a
virtual environment setup:
```shell
./install_packages.sh venv
```

After this, you should be able to run the self-test of the parser:
```shell
show_log_examples
```

It is not necessary to activate the virtual environment to run the scripts shown below.

## Searching files locally

In order to test the basic functionality or as a quick check across a number of log files,
you can "search" files which will search against the ETL ID and message of every log record.

Examples:
```shell
# built-in examples
log_search ERROR examples
# local files
log_search FD1B9A50D12C41C3 arthur.log*
# remote files (specified by prefix)
log_search 'finished successfully' s3://example-bucket/logs/
```

## Uploading log records from files manually

To leverage your Elasticsearch service domain, have the log records indexed.

Example:
```shell
# built-in examples
log_upload examples
# local files
log_upload arthur.log
# remote files (specified by prefix)
log_upload s3://example/logs/df-pipeline-id
```

## Automatic upload from S3

When the ETL is scheduled through the data pipeline, log files are automatically uploaded to S3.
We take advantage of this to trigger Lambda functions that parse the new log files and
add the log records to an ES domain.

### Create lambda and configure trigger

Set a trigger to have an S3 `PUT` call the Lambda function


### Upload code using deployment package

Create the deployment package:
```
./create_deployment.sh venv
```

See https://aws.amazon.com/blogs/security/how-to-control-access-to-your-amazon-elasticsearch-service-domain/
