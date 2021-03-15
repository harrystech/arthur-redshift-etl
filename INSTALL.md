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
    * It is important to setup a **default region** in case you are not using `us-east-1`.
    * Leave the `output` alone or set it to `json`.
```shell
aws configure
```

## Installation

### Cloning the repo

```shell
git clone git@github.com:harrystech/arthur-redshift-etl.git
```

## Building the Docker image

```shell
cd ../arthur-redshift-etl
git pull

bin/build_arthur.sh
```
You will now have an image `arthur-redshift-etl:latest`.

## Using the Docker container

It's easiest to set these environment variables,
_e.g._ in your `~/.bashrc` or `~/.zshrc` file:
```shell
export DATA_WAREHOUSE_CONFIG= ...
export ARTHUR_DEFAULT_PREFIX= ...
export AWS_DEFAULT_PROFILE= ...
export AWS_DEFAULT_REGION= ...
```

Then you can simply run:
```shell
bin/run_arthur.sh
```

You can set or override the settings on the command line:
```shell
bin/run_arthur.sh -p aws_profile ../warehouse-repo/config-dir wip
```

When in doubt, ask for help:
```shell
bin/run_arthur.sh -h
```

## Deploying to S3

You should now try to deploy your code into the object store in S3:
```shell
bin/deploy_arthur.sh
```

## Additional steps for developers

Ok, so even if you want to work on the ETL code, you should *first* follow the steps above to get to a running setup.
This section describes what *else* you should do when you want to develop here.

### Installing other requirements

#### Spark

**NOTE** Using Spark to extract data has been deprecated -- use Sqoop in an EMR cluster instead.

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

#### Additional packages

While our EC2 installations will use `requirements.txt` (see [bootstrap.sh](./bin/bootstrap.sh)),
you should always use `requirements-dev.txt` for local development. The packages listed in that
file are installed when building the Docker image.

### Running unit tests and type checker

See the [Formatting code](https://github.com/harrystech/arthur-redshift-etl/blob/next/README.md#formatting-code)
section in the README file.

Keep this [cheat sheet](http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html) close by for help with types etc.
