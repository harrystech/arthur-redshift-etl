**ETL Code for Loading Data Into a Redshift-based Data Warehouse**

```
  _    _                       _       _____          _     _     _  __ _     ______ _______ _
 | |  | |                     ( )     |  __ \        | |   | |   (_)/ _| |   |  ____|__   __| |
 | |__| | __ _ _ __ _ __ _   _|/ ___  | |__) |___  __| |___| |__  _| |_| |_  | |__     | |  | |
 |  __  |/ _` | '__| '__| | | | / __| |  _  // _ \/ _` / __| '_ \| |  _| __| |  __|    | |  | |
 | |  | | (_| | |  | |  | |_| | \__ \ | | \ \  __/ (_| \__ \ | | | | | | |_  | |____   | |  | |____
 |_|  |_|\__,_|_|  |_|   \__, | |___/ |_|  \_\___|\__,_|___/_| |_|_|_|  \__| |______|  |_|  |______|
                          __/ |
                         |___/
```

This README outlines how to get started.
You are probably looking for the [Wiki](https://github.com/harrystech/harrys-redshift-etl/wiki) pages,
which include a lot more information about the ETL and what it does (and why it does what it does).
And if something appears amiss, check out the [issues page](https://github.com/harrystech/harrys-redshift-etl/issues).

# Running the ETL

## Pre-requisites

What else you need in order to use this ETL depends on where you'd like run it and whether you anticipate
making changes to the ETL code.

### Spark

Install Spark if you'd like to be able to test jobs on your local machine.
On a Mac, simply use `brew install apache-spark` for an easy [Homebrew](http://brew.sh/) installation.

Our ETL code base includes everything to run the ETL in a Spark cluster on AWS
using [Amazon EMR](https://aws.amazon.com/elasticmapreduce/) so that local testing may be less important to you.
Running it locally offers shorter development cycles while running it in AWS means less network activity (going in
and out of your VPC).

### JAR files

Download the JAR files for the following software before deployment into an AWS Spark Cluster in order
to be able to connect to PostgreSQL databases and write CSV files for a Dataframe:

| Software | Version | JAR file  |
|---|---|---|
| [PostgreSQL JDBC](https://jdbc.postgresql.org/) driver | 9.4 | [postgresql-9.4.1208.jar](https://jdbc.postgresql.org/download/postgresql-9.4.1208.jar) |
| [Spark-CSV package](https://spark-packages.org/) | 1.4 | [spark-csv_2.10-1.4.0.jar](http://repo1.maven.org/maven2/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar) |
| [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) | 1.4 | [commons-csv-1.4.jar](http://central.maven.org/maven2/org/apache/commons/commons-csv/1.4/commons-csv-1.4.jar) |
| [Redshift JDBC](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver) | 1.1.10 | [RedshiftJDBC41-1.1.10.1010.jar](https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.10.1010.jar) |

Additionally, you'll need the following JAR files when running Spark jobs **locally** and want to push files into S3:

| Software | Version | JAR file  |
|---|---|---|
| [Hadoop AWS](https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/fs/s3native/NativeS3FileSystem.html) | 2.7.1 | [hadoop-aws-2.7.1.jar](http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar) |
| [AWS Java SDK](https://aws.amazon.com/sdk-for-java/) | 1.7.4 | [aws-java-sdk-1.7.4.2.jar](http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4.2/aws-java-sdk-1.7.4.2.jar) |

Do not upload these last two into EMR because EMR comes with all the correct versions out of the box.

The JAR files should be stored into the `jars` directory locally and in the `jars` folder of your selected
environment (see below).  A bootstrap action will copy them from S3 to the EMR cluster.

_Hint_: There is a download script in `bin/download_jars.sh` to pull the versions with which the ETL was tested.

### Python virtual environment

Our ETL code is using [Python3](https://docs.python.org/3/) so you may have to install that first.
On a Mac, simply use `brew install python3` for an easy [Homebrew](http://brew.sh/) installation.

We strongly suggest that you work within a virtual environment, so do `brew install virtualenv` or `pip install virtualenv`.

In order to run the Python code locally, you'll need to create a virtual environment with these additional packages:
* [Psycopg2](http://initd.org/psycopg/docs/) to connect to PostgreSQL and Redshift easily
* [boto3](https://boto3.readthedocs.org/en/latest/) to interact with AWS
* [PyYAML](http://pyyaml.org/wiki/PyYAML) for configuration files
* [jsonschema](https://github.com/Julian/jsonschema) for validating configurations and table design files
* [simplejson](https://pypi.python.org/pypi/simplejson/) for dealing with YAML files that are really just JSON

These are listed in the `requirements.txt` file that is used when building out the virtual
environment (e\.g\. during a bootstrap action in the EMR cluster).

**Note** this assumes you are in the *top-level* directrory of the Redshift ETL.

```shell
mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements.txt
python3 setup.py develop
```

_Hint_: Don't worry if you get assertion violations while building a wheel for PyYAML.

Consider installing [iPython](https://ipython.org/index.html).
```shell
pip3 install ipython
```

The EMR releases 4.5 and later include python3 so there's no need to install Python 3 using a bootstrap action.

**TODO** The above steps for a virtual environment should be merged with `bin/bootstrap.sh`.

#### Adding PySpark to your IDE

The easiest way to add PySpark so that code completion and type checking works while working on ETL code
might be to just add a pointer in the virtual environment.

First, activate your Python virtual environment so that the `VIRTUAL_ENV` environment variable is set.

Then, with Spark 2.0.0, try this for example:
```shell
cat > $VIRTUAL_ENV/lib/python3.5/site-packages/_spark_python.pth <<EOF
/usr/local/Cellar/apache-spark/2.0.0/libexec/python
/usr/local/Cellar/apache-spark/2.0.0/libexec/python/lib/py4j-0.10.1-src.zip
EOF
```

## Configuring the ETL (and upstream sources)

The best approach is probably to have another repo that contains the configuration file
and all the table design files and transformations.

### Redshift cluster and users

Although the Redshift cluster can be administered using the AWS console and `psql`, some
helper scripts will make setting up the cluster consistently much easier.
(See below for `initialize` and `create_user`.)

Also, add the AWS IAM role that the database owner may assume within Redshift
to your settings file so that Redshift has the needed permissions to access the
folder in S3.

### Sources

See the [Wiki](https://github.com/harrystech/harrys-redshift-etl/wiki) pages about
a description of configurations.


## Initializing the Redshift cluster

| Sub-command   | options |
| ------------- | ----------------------------------------------------- |
| initialize  | config, password, skip-user-creation |
| add_user    | config, etl-user, add-user-schema, skip-user-creation |

### Initial setup

After starting up the cluster, create groups, users and schemas (one per upstream source):

```shell
arthur.py initialize
```

### Adding users

Additional users may be added to the ETL or (analytics) user group:

```shell
arthur.py create_users
```

## Running the ETL

### Prerequisites for running the ETL in a cluster

All commands below will assume that you run `arthur.py` locally.  But if you want to
run the ETL in an EMR cluster instead, then you need to create a file with the
credentials that the cluster will need (a list of environment variables), then copy files needed in
the cluster and launch the cluster.

```shell
export DATA_WAREHOUSE_CONFIG='path to directory with config files and credentials'
bin/copy_env.sh 'name of your S3 bucket' local $USER
bin/aws_emr_cluster.sh -i 'name of your S3 bucket'
```

### Overview

Normally, the ETL will move data from upstream sources using the `dump` and `load` commands.

While working on the table designs or SQL for views and CTASs expressions, the steps are:
* `design`
* `sync`
* `load` *or*
* `update`


| Sub-command   | options |
| ------------- | ----------------------------------------------------- |
| design  | config, dry-run, target, table design dir |
| sync    | config, dry-run, prefix or env, target, table design dir, git-modified |
| dump    | config, dry-run, prefix or env, target |
| load, update | config, dry-run, prefix or env, target, explain-plan |
| etl | config, dry-run, prefix or env, target, force |

* Also `--verbose` or `--silent`
* Still needed ? `--force`

**Notes**

* Commands expect a config file.
* Commands accept `--dry-run` command line flag.
* Commands allow to specify a selector (specific source or table name(s)).
* Commands allow either `--prefix` or `--env` to select a folder in the S3 bucket.
* Log files are by default in `etl.log`.
* To copy data, use `aws s3 --recursive`.


### Setting up table designs

Use `design` to bootstrap any table design files based on the tables found in your source schemas.

```shell
arthur.py design
```

### Copying data out of PostgreSQL and into S3

Now download the data (leveraging a Spark cluster) using:

```shell
arthur.py dump
```

## Copying data from S3 into Redshift

Loading data includes creating or replacing tables and views as needed along the way:

```shell
arthur.py load
```

## Update (or create) views and tables based on queries (CTAS)

Update in place table or rewrite views:

```shell
arthur.py update
```

# Debugging and Contributing

Pull requests are welcome!

* Please run code through [pep8](https://www.python.org/dev/peps/pep-0008/) (see [local config](.pep8)):
```shell
pep8 python
```

* Even better, set up the git pre-commit hook to prevent you from accidentally breaking convention
```shell
ln -s -f ../../githooks/pre-commit ./.git/hooks/pre-commit
```

* Please have meaningful comments and git commit messages
(See [Chris's blog](http://chris.beams.io/posts/git-commit/)!)

# Tips & Tricks

## EMR login

You can use the `.ssh/config` file to pick the correct user (`hadoop`) for the cluster and to automatically
pick up a key file (replace the `IdentityFile` value with the location of the key pair file).

```
Host ec2-*.amazonaws.com
  ServerAliveInterval 60
  User hadoop
  IdentityFile ~/.ssh/emr-cluster-key.pem
```

## Development

Re-install the ETL code after pulling a new version. (Especially changes in scripts may not get picked up until you do.)
