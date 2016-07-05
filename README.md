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
- [PostgreSQL JDBC](https://jdbc.postgresql.org/) driver (9.4)
- [Spark-CSV package](https://spark-packages.org/)
- [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) (1.4)

Additionally, you'll need the following JAR files when running Spark jobs **locally** and want to store files in S3:
- [Hadoop AWS](https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/fs/s3native/NativeS3FileSystem.html) (2.7.1)
- [AWS Java SDK](https://aws.amazon.com/sdk-for-java/) (1.7.4)
(Do not upload them into EMR which comes with all the correct versions out of the box.)

The JAR files should be stored into the `jars` directory locally and in the `jars` folder of your selected
environment (see below).  A bootstrap action will copy them from S3 to the EMR cluster.

_Hint_: There is a download script in `bin/download_jars.sh` to pull the versions with which the ETL was tested.

### Python virtual environment

Our ETL code is using [Python3](https://docs.python.org/3/) so you may have to install that first.
On a Mac, simply use `brew install python3` for an easy [Homebrew](http://brew.sh/) installation.

In order to run the Python code locally, create a virtual environment and install additional packages:
* [Psycopg2](http://initd.org/psycopg/docs/) to connect to PostgreSQL and Redshift easily
* [boto3](https://boto3.readthedocs.org/en/latest/) to interact with AWS
* [PyYAML](http://pyyaml.org/wiki/PyYAML) for configuration files
* [jsonschema](https://github.com/Julian/jsonschema) for validating configurations and table design files

These are listed in the `python/requirements.txt` file that is used during a bootstrap action in the EMR cluster.

**Note** this assumes you are in the *top-level* directrory of the Redshift ETL.

```shell
mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade pip
pip3 install -r python/requirements.txt
(cd python && python3 setup.py develop)
```

_Hint_: Don't worry if you get assertion violations while building a wheel for PyYAML.

Consider installing [iPython](https://ipython.org/index.html).
```shell
pip3 install ipython
```

The EMR releases 4.5 and later include python3 so there's no need to install Python 3 using a bootstrap action.

TODO The above steps for a virtual environment should be merged with `bin/bootstrap.sh`.

### Adding PySpark to your IDE

The easiest way to add PySpark so that code completion and type checking works might be to just
add a pointer in the virtual environment. On a Mac with a Homebrew installation of Spark, try this:
```shell
cat > venv/lib/python3.5/site-packages/_spark_python.pth <<EOF
/usr/local/Cellar/apache-spark/1.6.1/libexec/python
/usr/local/Cellar/apache-spark/1.6.1/libexec/python/lib/py4j-0.9-src.zip
EOF
```

## Configuring the ETL (and upstream sources)

The best approach is probably to have another repo that contains the configuration file
and all the table design files and transformations.

### Redshift cluster and users

Although the Redshift cluster can be administered using the AWS console and `psql`, some
helper scripts will make setting up the cluster consistently much easier.
(See below for `initial_setup.py` and `create_user.py`.)

Also, add the AWS IAM role that the database owner may assume within Redshift
to your settings file so that Redshift has the needed permissions to access the
folder in S3.

#### Initial setup

After starting up the cluster, create groups, users and schemas (one per upstream source):

```shell
./initial_setup.py
```

#### Adding users

Additional users may be added to the ETL or (analytics) user group:

```shell
./create_users.py
```

### Sources

TODO

### ETL Configuration

TODO

## Running the ETL

### Setting up table designs

Use `dump_schemas_to_s3.py` to bootstrap any table design files based on the
tables found in your source schemas.

```shell
./dump_schemas_to_s3.py
```

### Copying data out of PostgreSQL and into S3

Now download the data (leveraging a Spark cluster) using `dump_data_to_s3.py`:

```shell
./dump_data_to_s3.py
```

## Copying data from S3 into Redshift

Loading data includes crreating tables as needed along the way:

```shell
./load_to_redshift.py
```

## Update (or create) views and tables based on queries (CTAS)

Once (raw) data is available, create additional views and tables (based on queries):

```shell
./update_in_redshift.py
```

# Debugging and Contributing

Pull requests are welcome!

* Please run code through [pep8](https://www.python.org/dev/peps/pep-0008/) (see [local config](.pep8)):
```shell
pep8 python
```

# Tips & Tricks

## EMR login

You can use the `.ssh/config` file to pick the correct user (`hadoop`) for the cluster and to automatically
pick up a key file.

```
Host ec2-*.amazonaws.com
  ServerAliveInterval 60
  User hadoop
  IdentityFile ~/.ssh/emr-spark-cluster.pem
```
