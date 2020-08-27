# Building and running the Docker image

This file describes the steps necessary to run ETLs using this codebase.

There's first a set of commands to run to start working on and with the ETL.
The following sections simply add more explanations (or variations).

## Pre-requisites

* You will need a way to clone this repo, _e.g._ the `git` command line tool.
* _Arthur_ runs inside a Docker container. So make sure to have [Docker](https://docs.docker.com/install/) installed.
    The software _can_ be installed directly into a virtual environment. But we no longer recommend that.
* You will also need an account with AWS and access using a profile. Be sure to have your access already configured:
    * If you have to work with multiple access keys, check out the support of profiles in the CLI.
    * It is important to setup a **default region** since the start scripts do not specify one.
    * Leave the `output` alone or set it to `json`.
```bash
aws configure
```

## Installation

### Cloning the repo

```bash
git clone git@github.com:harrystech/arthur-redshift-etl.git
```

## Building the Docker image

```bash
cd ../arthur-redshift-etl
git pull

bin/build_arthur.sh
```
You will now have an image `arthur-redshift-etl:latest`.

## Using the Docker container

It's easiest to set these environment variables, _e.g._ in your `~/.bashrc` or file:
```bash
export DATA_WAREHOUSE_CONFIG= ...
export ARTHUR_DEFAULT_PREFIX= ...
export AWS_DEFAULT_PROFILE= ...
```

Then you can simply run:
```bash
bin/run_arthur.sh
```

You can set or override the settings on the command line:
```bash
bin/run_arthur.sh -p aws_profile ../warehouse-repo/config-dir wip
```

When in doubt, ask for help:
```bash
bin/run_arthur.sh -h
```

## Deploying to S3

You should now try to deploy your code into the object store in S3:
```bash
bin/deploy_arthur.sh
```

## Additional steps for developers

Ok, so even if you want to work on the ETL code, you should *first* follow the steps above to get to a running setup.
This section describes what *else* you should do when you want to develop here.

### Installing other requirements

#### Spark

**NOTE** Using Spark to extract data has been deprecated -- use Sqoop in an EMR cluster instead.

Install Spark if you'd like to be able to test jobs on your local machine.
On a Mac, simply use `brew install apache-spark`.

Our ETL code base includes everything to run the ETL in a Spark cluster on AWS
using [Amazon EMR](https://aws.amazon.com/elasticmapreduce/) so that local testing may be less important to you.
Running it locally offers shorter development cycles while running it in AWS means less network activity (going in
and out of your VPC).

#### Drivers (JAR files)

Download the JAR files for the following software before deployment into an AWS Spark Cluster in order
to be able to connect to PostgreSQL databases and write CSV files for a Dataframe:

| Software | Version | JAR file  |
|---|---|---|
| [PostgreSQL JDBC](https://jdbc.postgresql.org/) driver | 9.4 | [postgresql-9.4.1208.jar](https://jdbc.postgresql.org/download/postgresql-9.4.1208.jar) |
| [Redshift JDBC](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver) | 1.2.1 | [RedshiftJDBC41-1.2.1.1001.jar](https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.2.1.1001.jar) |

Additionally, you'll need the following JAR files when running Spark jobs **locally** and want to push files into S3:

| Software (local) | Version | JAR file  |
|---|---|---|
| [Hadoop AWS](https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/fs/s3native/NativeS3FileSystem.html) | 2.7.1 | [hadoop-aws-2.7.1.jar](http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar) |
| [AWS Java SDK](https://aws.amazon.com/sdk-for-java/) | 1.7.4 | [aws-java-sdk-1.7.4.2.jar](http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4.2/aws-java-sdk-1.7.4.2.jar) |

Do not upload these last two into EMR because EMR comes with all the correct versions out of the box.

The JAR files should be stored into the `jars` directory locally and in the `jars` folder of your selected
environment (see below).  A bootstrap action will copy them from S3 to the EMR cluster.

_Hint_: There is a download script in `bin/download_jars.sh` to pull the versions with which the ETL was tested.

The EMR releases 4.5 and later include `python3` so there's no need to install Python 3 using a bootstrap action.

##### Adding PySpark to your IDE

The easiest way to add PySpark so that code completion and type checking works while working on ETL code
might be to just add a pointer in the virtual environment.

First, activate your Python virtual environment so that the `VIRTUAL_ENV` environment variable is set.

Then, with Spark 2.1.1, try this for example:
```bash
cat > $VIRTUAL_ENV/lib/python3.5/site-packages/_spark_python.pth <<EOF
/usr/local/Cellar/apache-spark/2.1.1/libexec/python
/usr/local/Cellar/apache-spark/2.1.1/libexec/python/lib/py4j-0.10.4-src.zip
EOF
```

#### Additional packages

While our EC2 installations will use `requirements.txt` (see [bootstrap.sh](./bin/bootstrap.sh)),
you should always use `requirements-dev.txt` for local development. The packages listed in that
file are installed when building the Docker image.

### Running unit tests and type checker

Here is how to run the static type checker [mypy](http://mypy-lang.org/) and doctest:
```bash
run_tests.py
```

Keep this [cheat sheet](http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html) close by for help with types etc.

See also the [Formatting code](https://github.com/harrystech/arthur-redshift-etl/blob/next/README.md#formatting-code)
section in the README file.
