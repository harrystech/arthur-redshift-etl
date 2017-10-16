# Overview

The goal of the log processing is to make the logs from Arthur ETLs
available in Kibana (via Elasticsearch) in order to have dashboards
for some key metrics of the ETL, like
    * Top N sources that take the most time to extract
    * Top N relations that take the most time to load
    * Number of warnings or errors

## Setup and Requirements

### Amazon Elasticsearch Service Domains

You have to have an Elasticsearch sercie running.
For more information about Elasticsearch in AWS, see [Getting Started Guide](http://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-gsg.html).
See the cloudformation directory for an example setup.

You will have to use the configure option to store the endpoint address in the
parameter store.

### Lambda Permissions

To use the lambda function to automatically upload to ES any log files to show up in S3,
you will have to create a new role to use with the lambda.

The role must have these permissions:
* `AWSLambdaBasicExecutionRole`
* Read permissions for your S3 bucket
* Write permissions for your ES domain

TODO Create role automatically, `temp-log-parser`, add to config


## Installation

This code uses Python 3. See the [toplevel README](../README.md) for installation instructions.

In order to run this code locally or to upload it as a lambda function, you have to have a
virtual environment setup:
```shell
./install_packages.sh venv
```

It is not necessary to activate the virtual environment to run the scripts shown below.

## Testing

The individual steps (parsing, compiling, uploading) can be tested locally.

### Parsing example log lines

You should be able to run the self-test of the parser:
```shell
show_log_examples
```

### Searching files locally

In order to test the basic functionality or as a quick check across a number of log files,
you can "search" files which will search against the ETL ID, log level and message of every log record.

Examples:
```shell
# built-in examples
search_log ERROR examples
# local files
search_log FD1B9A50D12C41C3 ../arthur.log*
# remote files (specified by prefix)
search_log 'finished successfully' s3://example-bucket/logs/
```

### Configure endpoints

XXX todo

Need to pass in the "environment type" which comes from the VPC, like `dev`.
Sets endpoint for env and also for bucket (so that lambda can use it).

```shell
config_log set_endpoint dev "your endpoint:443"
config_log get_endpoint dev
```

### Uploading log records from files manually

To leverage your Elasticsearch service domain, have the log records indexed.

Need to pass in the "environment type" which comes from the VPC, like `dev`.

Example:
```shell
# built-in examples
upload_log dev examples
# local files
upload_log dev ../arthur.log
# remote files (specified by prefix)
upload_log dev s3://example/logs/df-pipeline-id
```

### Deleting older indices

XXX todo
Should be called at least once a week.

```shell
config_log list_indices dev
config_log delete_old_indices dev
```

## Automatic upload from S3

When the ETL is scheduled through the data pipeline, log files are automatically uploaded to S3.
We take advantage of this to trigger Lambda functions that parse the new log files and
add the log records to an ES domain.

### Create lambda and configure trigger

#### Create the deployment package

```
./create_deployment.sh venv
```

#### Create or update the Lambda function

* Upload zip file just created.
* Set handler to `etl_log_processing.upload.lambda_handler`
* Set tags for `user:project` to `data-warehouse`

#### Trigger

Set a trigger to have an S3 `PUT` call the Lambda function ("object created")


See https://aws.amazon.com/blogs/security/how-to-control-access-to-your-amazon-elasticsearch-service-domain/
