This file describes the steps necessary to run ETLs using this codebase.

# Just the facts...

## ... for installing the ETL code

Here's a set of commands to run to start working on and with the ETL.
The paragraphs below simply add more explanations and variations.
The only pre-requisite here is that you have `homebrew` installed for your (macOS) laptop.

```
# Setup repo
git clone git@github.com:harrystech/arthur-redshift-etl.git
cd arthur-redshift-etl

# Setup tools -- initial
brew install awscli
brew install jq

# Setup tools -- later
brew upgrade awscli
brew install jq

# Setup python3 with virtualenv
brew install python3
pip3 install virtualenv
pip3 install pip --upgrade --disable-pip-version-check

# Setup virtualenv (with wrapper)
export WORKON_HOME=~/Envs
export VIRTUALENVWRAPPER_PYTHON=/usr/local/bin/python3
source /usr/local/bin/virtualenvwrapper.sh

mkvirtualenv --python=python3 arthur-redshift-etl
workon .
pip3 install --requirement ./requirements-dev.txt
python3 setup.py develop
```

The next step is to setup your AWS credentials, organized in profiles probably.
But if you're brave enough to skip the explanations and just run the above commands,
you (hopefully?) know what this entails.

## ... for updating the ETL code

After you pull a new version of Arthur, you should re-install the ETL code.
This is especially important to pick up changes in the scripts!
```shell
# Make sure the virtual environment is active, then
python3 setup.py develop
```

If packages changed, make sure to install those using `pip3`, e.g.
```
pip3 install --upgrade --requirement requirements-dev.txt
```

# Getting ready to run ETLs

What all you need in order to use this ETL tool depends on where you'd like run it and whether you anticipate
making changes to the ETL code.

## Pre-requisites for "end users"

Let's go through your setup steps in order to run the CLI, `arthur.py`, assuming that you will not work on the code base.

### Clone this project

Should be obvious ... but you need to `git clone` this repo using the handy link in Github.

### AWS CLI

We are using some shell scripts that use the AWS CLI and so this must be installed. We strongly suggest
that you use a packaging tool, like [Homebrew](https://brew.sh/) on macOS.
```shell
brew install awscli
aws --version
# Should be better than 1.10
```

In order to interact with the S3 bucket with ETL data, a.k.a. your object store, you will need
to set up AWS credentials.
```shell
aws configure
```

* If you have to work with multiple access keys, check out the support of profiles in the CLI.
* It is important to setup a **default region** since the start scripts do not specify one.
* Leave the `output` alone or set it to `json`.
* To test, try to list the contents of your company's object store:
```shell
aws s3 ls 's3://<your s3 bucket>'
```

### Other commands

We also use a tool called [jq](https://stedolan.github.io/jq/manual/v1.5/) to help parse responses
from AWS CLI commands.
```shell
brew install jq
```

### Python and virtual environment

Our ETL code is using [Python3](https://docs.python.org/3/) so you may have to install that first.
On a Mac, simply use [Homebrew](http://brew.sh/) for an easy installation.
We strongly suggest that you always work within a virtual environment.
```shell
brew install python3
# Using the newly installed pip3
pip3 install virtualenv
```

For new versions of pip, make sure to run this **outside** the virtual environment:
```
pip3 install pip --upgrade --disable-pip-version-check
```

To run code locally, you'll need to create a virtual environment with additional packages.
These packages are listed (with their expected versions) in the `requirements.txt` file.
The most prominent packages are:
* [Psycopg2](http://initd.org/psycopg/docs/) to connect to PostgreSQL and Redshift easily
* [boto3](https://boto3.readthedocs.org/en/latest/) to interact with AWS
* [PyYAML](http://pyyaml.org/wiki/PyYAML) for configuration files
* [simplejson](https://pypi.python.org/pypi/simplejson/) for dealing with YAML files that are really just JSON
* [jsonschema](https://github.com/Julian/jsonschema) for validating configurations and table design files
And in development:
* [mypy](http://mypy-lang.org/) for static type checking

The packages listed in `requirements-dev.txt` should be loaded into development environments
and include the others. While our EC2 installations will use `requirements.txt` (see [bootstrap.sh](./bin/bootstrap.sh)),
you should always use `requirements-dev.txt` for local development.

#### Using vanilla virtualenv

**Note** this assumes you are in the **top-level** directory of the Redshift ETL.

```shell
mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --requirement ./requirements-dev.txt
python3 setup.py develop
```

_Hint_: Don't worry if you get assertion violations while building a wheel for PyYAML.

#### Using virtualenv wrapper

Feel free to use [`virtualenv-wrapper`](https://virtualenvwrapper.readthedocs.io/en/latest/) to make
your life switching in and out of virtual environments easier.

Assuming you already have setup your environment to take advantage of the virtualenv wrapper tool
by
* setting the environment variable `WORKON_HOME`
* setting the environment variable `VIRTUALENVWRAPPER_PYTHON` to `/usr/local/bin/python3`
* making sure that `/usr/local/bin/virtualenvwrapper.sh` is _source_d in your shell profile.

**Creating virtual env if you don't plan on changing the ETL code**

First, change into the directory where your data warehouse setup will live, then:
```shell
mkvirtualenv --python=python3 -a `pwd` dw
```
The option `-a` will send you into this directory everytime you run:
```
workon dw
```

**Creating virtual env as an ETL developer**

Use the same name for the virtual env that is the name of the repo:
```shell
mkvirtualenv --python=python3 arthur-redshift-etl
```
This will make it easier later when you're in the directory to say:
```
workon .
```

**Updating the environment (under either scenario)**
```
pip3 install --requirement ./requirements-dev.txt
python3 setup.py develop

# Make sure to check the path below
echo "source `\cd ../arthur-redshift-etl/etc && \pwd`/arthur_completion.sh" >> $VIRTUAL_ENV/bin/postactivate
```

Jumping ahead bit, if you want to use a default environment other than your login name, use something like this:
```
echo "export ARTHUR_DEFAULT_PREFIX=experimental" >> $VIRTUAL_ENV/bin/postactivate
```

(This is different from something you will set for the `AWS_PROFILE`.)

## Additional pre-requisites for developers

Ok, so even if you want to work on the ETL code, you should *first* follow the steps above to get to a running setup.
This section describes what *else* you should do when you want to develop here.

### Spark

Install Spark if you'd like to be able to test jobs on your local machine.
On a Mac, simply use `brew install apache-spark`.

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

The EMR releases 4.5 and later include python3 so there's no need to install Python 3 using a bootstrap action.

#### Adding PySpark to your IDE

The easiest way to add PySpark so that code completion and type checking works while working on ETL code
might be to just add a pointer in the virtual environment.

First, activate your Python virtual environment so that the `VIRTUAL_ENV` environment variable is set.

Then, with Spark 2.1.1, try this for example:
```shell
cat > $VIRTUAL_ENV/lib/python3.5/site-packages/_spark_python.pth <<EOF
/usr/local/Cellar/apache-spark/2.1.1/libexec/python
/usr/local/Cellar/apache-spark/2.1.1/libexec/python/lib/py4j-0.10.4-src.zip
EOF
```

## Running unit tests and type checker

Here is how to run the static type checker [mypy](http://mypy-lang.org/) and doctest:
```shell
run_tests.py

# And in case you have a config file handy
arthur.py selftest
```

Keep this [cheat sheet](http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html) close by.
