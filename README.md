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

This README outlines how to get started with the ETL.
This includes information about setting up _Arthur_ which is the driver for ETL activities.

You are probably (also) looking for the [Wiki](https://github.com/harrystech/harrys-redshift-etl/wiki) pages,
which include a lot more information about the ETL and what it does (and why it does what it does).
And if something appears amiss, check out the [issues page](https://github.com/harrystech/harrys-redshift-etl/issues).

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

In order to interact with the S3 bucket with ETL data, a.k.a. your data lake, you will need
to set up credentials.
```shell
aws configure
```
* If you have to work with multiple access keys, check out the support of profiles in the CLI.
* It is important to setup a **default region** since the start scripts do not specify one.
* To test, try to list the contents of your company's data lake:
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
* [mypy](http://mypy-lang.org/) for static type checking

For running the ETL remotely in EC2, the `bin/bootstrap.sh` script will take care of the creation
of the virtual environment.

#### Using vanilla virtualenv

**Note** this assumes you are in the **top-level** directory of the Redshift ETL.

```shell
mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --requirement ./requirements.txt --disable-pip-version-check
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
mkvirtualenv --python=python3 harrys-redshift-etl
```
This will make it easier later when you're in the directory to say:
```
workon .
```

**Updating the environment (under either scenario)**
```
pip3 install --requirement ./requirements.txt
python3 setup.py develop

# Make sure to check the path below
echo "source `\cd ../harrys-redshift-etl/etc && \pwd`/arthur_completion.sh" >> $VIRTUAL_ENV/bin/postactivate
```

Jumping ahead bit, if you want to use a default environment other than your login name, use _something like` this:
```
echo "export ARTHUR_DEFAULT_PREFIX=experimental" >> $VIRTUAL_ENV/bin/postactivate
```

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

## Configuring the ETL (and upstream sources)

The best approach is to have a separate repo for your data warehouse that contains the configuration files
and all the table design files and transformations.  The documentation will in many places assume
that you have a "*sibling*" repo so that when within the repo for your local data warehouse (with
configuration, credentials, and table designs), you can simply use `../harrys-redshift-etl/` to find
your way back to this ETL code.

### Redshift cluster and users

Although the Redshift cluster can be administered using the AWS console and `psql`, some
helper scripts will make setting up the cluster consistently much easier.
(See below for `initialize` and `create_user`.)

Also, add the AWS IAM role that the database owner may assume within Redshift
to your settings file so that Redshift has the needed permissions to access the
folder in S3.
(And don't forget to add the role to the list of known IAM roles in Redshift.)

### Sources

See the [Wiki](https://github.com/harrystech/harrys-redshift-etl/wiki) pages about
a description of configurations.

## Running the ETL (`arthur.py`)

### General notes about the CLI

* Commands expect a config file and will start by picking up all files in a local `./config` directory.
* Commands accept `--dry-run` command line flag to test without modifying the environment.
* Most commands allow to specify glob patterns to select specific schema(s) or table(s).
* Most commands use `--prefix` to select a folder in the S3 bucket (but you may also have to use the `--remote` option).
* To pick a prefix without specifying it every time, set the environment variable `ARTHUR_DEFAULT_PREFIX`.
* Log files are by default in `arthur.log`.  They are rotated and deleted so that your disk doesn't fill up too much.
* To see more log information, use `--verbose`. To see less, use `--quiet`.
* To see them formatted in the console the same way as they are formatted in the log files, use `--prolix`.
* You could copy data manually, but you probably shouldn't and let `arthur.py sync` manage files.
* You can use environment variables to pass in credentials for database access, but you should probably use a file for that.

### Prerequisites for running the ETL in a cluster

#### Creating a credentials file

All credentials can be picked up from environment variables by the ETL. Instead of setting these
variables before starting the ETL, you can also add a file with credentials to the `config` directory
where the ETL will pick them up for you. The credentials file should be formatted just like a shell
file used to setting variables, meaning lines should have the forms:
```
# Lines with '#' are ignored.
NAME=value
# Although not meaningful within the ETL code, you can use the "export" syntax from Bash
export NAME=value
```

The minimal credentials file contains the login information for the ETL user that Arthur will use
to execute in Redshift.  Make sure this file exists in your data warehouse repo as `config/credentials.sh`:
```
DATA_WAREHOUSE_ETL=postgres://etl:<password>@<host>:<port>/<dbname>?sslmode=require
```

#### Copying code into the S3 bucket

Copy the ETL code (including bootstrap scripts and configuration):
```shell
export DATA_WAREHOUSE_CONFIG="<path to directory with config files and credentials>"
# export DATA_WAREHOUSE_CONFIG="\cd ./config && \pwd`
bin/upload_env.sh "<your S3 bucket>" $USER
```

#### Starting a cluster and submitting commands

Start a cluster (which needs to know about your S3 bucket where the bootstrap code lives):
```shell
bin/aws_emr_cluster.sh -i "<your S3 bucket>"
```

Now check for the output and pick up the cluster ID. There will be a line that looks something like this:
```
+ CLUSTER_ID=j-12345678
```

You can then use `arthur.py --submit "<cluster ID>"` instead of `arthur.py` in the examples below.
Note that the `--submit` option must be between `arthur.py` and the sub-command in use, e.g.
```shell
arthur.py --submit "<cluster ID>" load --prolix --prefix $USER
```

### Initializing the Redshift cluster

| Sub-command   | Goal |
| ---- | ---- |
| `initialize`  | Create schemas, groups and users |
| `create_user`    | Create (or configure) users that are not mentioned in the configuration file |

```shell
# The commands to setup the data warehouse users and groups or any database is by ADMIN (connected to `dev`)
arthur.py initialize  # NOP but errors out
arthur.py initialize development --with-user-creation  # Must create users and groups on first call
```

### Starting with design files (and managing them)

| Sub-command   | Goal |
| ---- | ---- |
| `design`  | Download schemas from upstream sources and bootstrap design files |
| `auto_design`  | Bootstrap design files for transformations based on new SQL queries |
| `explain`  | Review query plan for transformations |
| `validate`  | After making changes to the design files, validate that changes are consistent with the expected format and with respect to each other |
| `sync` | Upload your local files to your data lake |

```shell
arthur.py sync "<schema>"  # This will upload local files related to one schema into your folder inside the S3 bucket
arthur.py sync "<schema>.<table>"  # This will upload local files for just one table
```

### Loading and updating data

| Sub-command   | Goal |
| ---- | ---- |
| `extract`  | Get data from upstream sources |
| `load`, `upgrade` | Make data warehouse "structural" changes and let data percolate through |
| `update` | Move data from upstream sources and let it percolate through |
| `unload` | Take data from a relation in the data warehouse and extract as CSVs into S3 |

```shell
arthur.py extract
arthur.py load  # This will automatically create schemas and tables as necessary
```

### Dealing with schemas (create, restore)

| Sub-command   | Goal |
| ---- | ---- |
| `create_schemas`  | Create schemas; normally `load` will do that for you |
| `restore_schemas`  | Bring back schemas from backup if `load` was aborted |


### Working with subsets of tables

| Sub-command   | Goal |
| ---- | ---- |
| `show_dependents`  | Inspect the other relations impacted by changes to the selected ones |
| `show_dependency_chain`  | Inspect which other relations feed the selected ones |

The patterns used by commands like `extracat` or `load` may be provided using files.
Together with `show_dependents` and `show_dependency_chains`, this opens up
opportunities to work on a "sub-tree" of the data warehouse.

#### Working with just the source schemas or transformation schemas

At the beginning it might be worthwhile to focus just on tables in source schemas -- those
tables that get loaded using CSV files after `extract`.
```shell
arthur.py show_dependents -q | grep 'kind=DATA' | tee sources.txt

# List CSV files and manifests, then continue with upgrade etc.
arthur.py ls --remote --only @sources.txt
```

The above also works when we're just interested in transformations. This can
save time while iterating on transformations since the sources won't be reloaded.

Example:
```shell
arthur.py show_dependents -q | grep -v 'kind=DATA' | tee transformations.txt

arthur.py sync @transformations.txt
arthur.py upgrade --only @transformations.txt
```

#### Working with a table and everything feeding it

While working on transformations or constraints, it might be useful to focus on just a
set of of tables that feed data into it.

Example:
```shell
arthur.py show_dependency_chain -q www.users | tee www_users.txt

arthur.py sync www.users
arthur.py upgrade --only @www_users.txt
```

## Working with a staging environment

A staging environment can help with deploying data that you'll be confident to release into production.

```shell
arthur.py initialize staging
arthur.py initialize staging --dry-run  # In case you want to see what happeens but not lose all schemas.
```

Once everything is working fine in staging, you can promote the code into production.
```shell
./bin/sync_env.sh "<your S3 bucket>" staging production
```

# Contributing and Releases

## Creating pull requests

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
(See [Chris's blog](http://chris.beams.io/posts/git-commit/))
* Use git rebasing to merge your commits into logical chunks
(See [Thoughtbot's guidelines](https://github.com/thoughtbot/guides/blob/master/protocol/git/README.md))

## Releasing new versions

Here are the basic steps to release a new version. Appropriate as you deem appropriate.

* Create a new branch for the release candidate, e.g. `v0_22_0` for v0.22.0.
(Do yourself a favor and use underscores in branch names and periods in the tag names.)

* Your first commit on this branch is a bump in the version number in `setup.py`.

* Create a pull request for your new branch.

* Go through pull requests that are ready for release and change their base branch to your release branch (in our example, `v0_22_0`).
    * Make sure the PR message contains the Issue number or the Jira story (like DW-99).
    * Add the changes from the story work into comments of the PR.
    * Then merge the ready PRs into your release candidate.

* Test then merge the PR with your release candidate into master.

* Create a release under [Releases](https://github.com/harrystech/harrys-redshift-etl/releases).
    * Create a new release and set the release version, e.g. `v0.22.0`.
    * Copy the comments from the PR where you collected all the changes into the release notes.
    * Save the release which will add the tag of the release.

* Ship the new version using `upload_env.sh`.

# Tips & Tricks

## Miscellaneous

### Using command completion in the shell

For the bash shell, there is a file to add command completion that allows to tab-complete schemas and table names.
```shell
source etc/arthur_completion.sh
```

If you are normally in the repo for your data warehouse configuration, then this might be better:
```shell
source ../harrys-redshift-etl/etc/arthur_completion.sh
```

And if you're using `virtualenv-wrapper`, then you should make this part of the activation sequence.

### iPython and q

Consider installing [iPython](https://ipython.org/index.html):
```shell
pip3 install ipython
```

Also, [q](https://github.com/zestyping/q) comes in handy for debugging:
```shell
pip3 install q
```

### Running unit tests and type checker

Here is how to run the static type checker [mypy](http://mypy-lang.org/) and doctest:
```shell
run_tests.py

# And in case you have a config file handy
arthur.py self-test
```

Keep this [cheat sheet](http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html) close by.

### EMR login / EC2 login

You can use the `.ssh/config` file to pick the correct user (`hadoop`) for the cluster and to automatically
pick up a key file.  Replace the `IdentityFile` value with the location of your key pair file.
```
Host ec2-*.amazonaws.com
  ServerAliveInterval 60
  User hadoop
  IdentityFile ~/.ssh/emr-cluster-key.pem
```

If you find yourself using a one-off EC2 instance more often than an EMR cluster, change the `User`:
```
  User ec2-user
```

### Using `develop` of `setup.py`

Re-install the ETL code after pulling a new version. (Especially changes in scripts may not get picked up until you do.)
```shell
pip3 install -r requirements.txt
python3 setup.py develop
```
