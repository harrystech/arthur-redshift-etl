# Overview

The goal of the log processing is to make the logs from Arthur ETLs
available in Kibana (via an Elasticsearch Service) in order to have dashboards
for some key metrics of the ETL, like
* Top N sources that take the most time to extract
* Top N relations that take the most time to load
* Number of warnings or errors
and to just more quickly get to the error message from validation pipelines.

# Requirements

## Amazon Elasticsearch Service Domains

You have to have an Elasticsearch service running.
For more information about Elasticsearch in AWS, see their [Getting Started Guide](http://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-gsg.html).

The `config_log` utility is used to store the endpoint address in the parameter store, see below for usage information.

See https://aws.amazon.com/blogs/security/how-to-control-access-to-your-amazon-elasticsearch-service-domain/

# Installation

## Lambda

This code uses Python 3. See the [toplevel INSTALL](../INSTALL.md) for general installation instructions.

In order to run this code locally or to upload it as a lambda function, you have to have a
virtual environment setup:
```shell
./update_virtual_env.sh venv
```

To deploy the lambda function, create a package
```shell
./create_deployment_package.sh venv
```

You need to upload the latest package to S3 in order to use it in the CloudFormation step:
```shell
aws s3 cp log_processing_*.zip s3://<your code bucket>/lambda/
```

## CloudFormation

You can use the included [`dw_es_domain.yaml`](./dw_es_domain.yaml) file
to bring up a ES domain along with a Lambda function to load log files.

```shell
../cloudformation/do_cloudformation.sh create dw_es_domain dev DomainName=dw-es-dev \
    CodeS3Bucket="<your code bucket>" CodeS3Key="<your latest zip file>" \
    NodeStorageSize=20 WhitelistCIDR1=192.168.1.1/32
```
Replace the IP address with your actual office IP address.

If you need to update the stack, e.g. to update the Lambda handler, modify this line appropriately:
```shell
../cloudformation/do_cloudformation.sh update dw_es_domain dev DomainName=UsePreviousValue \
    CodeS3Bucket=UsePreviousValue CodeS3Key=UsePreviousValue \
    NodeStorageSize=UsePreviousValue WhitelistCIDR1=UsePreviousValue
```
Remember that once you have set an optional parameter, you have to at least pass in that parameter
with `=UserPreviousValue` or it reverts to its default.

## Configuration

### S3 lambda notification

Since the bucket for log files is not part of the CloudFormation template, we have to manually add the trigger:
```shell
aws s3api put-bucket-notification-configuration --bucket "<your bucket>" --notification-configuration \
  '{"LambdaFunctionConfigurations":[{ "LambdaFunctionArn":"<your function arn>","Events":["s3:ObjectCreated:*"]}]}'
```

**Note** If you use S3 notifications on this bucket for something else, you must **add** them since the
notification configuration will be replaced.

### ES Endpoint

Need to pass in the "environment type" which comes from the VPC, like `dev`.
Sets endpoint for env and also for bucket (so that lambda can use it).

```shell
config_log set_endpoint dev "your bucket" "your endpoint:443"
config_log get_endpoint dev
```
The endpoint that is used here can be found as an output of the CloudFormation stack.

### Index template

If you need to update the index template:

```shell
config_log put_index_template dev
```
The template will be automatically set by the lambda handler with the first call.

## Deleting older indices

```shell
config_log get_indices dev
config_log delete_old_indices dev
```

## Kibana

In Kibana, add `dw-etl-logs-*` in **Management** -> **Index Patterns** and select `@timestamp` as the timestamp.

Also, it's probably best to use UTC instead of the browser time, so change in **Management** -> **Advanced Settings**:
```text
dateFormat:tz    UTC
dateFormat       YYYY/MM/DD HH:mm:ss.SSS
defaultIndex     dw-etl-logs-*
```

No further changes should be necessary.
We use `@timestamp` so tools like Timelion will pick up the timestamp automatically.

# Testing

The individual steps (parsing, compiling, uploading) can be tested locally.

## Parsing example log lines

You should be able to run the self-test of the parser:
```shell
show_log_examples
```

## Searching files locally

In order to test the basic functionality or as a quick check across a number of log files,
you can "search" files which will search against the ETL ID, log level and message of every log record.

Examples:
```shell
# built-in examples
search_log ERROR examples
# local files
search_log FD1B9A50D12C41C3 ../arthur.log*
# remote files
search_log 'finished successfully' s3://example-bucket/logs/example/StdError.gzip
```

## Uploading log records from files manually

You need to pass in the "environment type" which comes from the VPC, like `dev`,
so that the endpoint address can be looked up in the parameter store.

Example:
```shell
# built-in examples
upload_log dev examples
# local files
upload_log dev ../arthur.log
# remote files
upload_log dev s3://example/logs/df-pipeline-id/component/instance/attempt/StdError.gzip
```

# Loading log files from the past using Lambda

In case you find yourself already having log files that are in the bucket but not indexed,
then you can run the following script to invoke the lambda function:

```shell
./backfill_logfiles.py "<bucket_name>" "<prefix>" "<function_name>"
```
