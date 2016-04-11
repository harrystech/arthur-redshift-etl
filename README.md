# harrys-redshift-etl

ETL Code for Loading Data Into a Redshift-based Data Warehouse

## Overview

See [ETL Flow diagram](doc/etl_flow.svg) for a basic overview.  The idea
is that there are upstream PostgreSQL databases which should be collected
into a single data warehouse where their data is made available to BI users.

The tools in this repo fall into two categories:
    1. Prototyping code to bring up the warehouse and experiment with table designs.
    1. Production code to bring over data (on, say, a daily basis)

Either tool set expects that
* data sits in S3 bucket between dump and load,
* connection information is set using environment variables, and
* configuration file describes upstream data sources.

## Working with the Python ETL code to bring up a data warehouse

Step 1 is to bring up a data warehouse from scratch.
This will setup the database, schemas, users, groups.
Then we can start populating the data warehouse with data.

### Creating the protozoic data warehouse

**Note** that you'll have to have your Python environment set up,
see [Python README](python/README.md).

Make sure the virtual environment is active:
```shell
source venv/bin/activate
```
This will also pull in the scripts to your path.

### Creating a configuration file for the data warehouse

The configuration file describes
* Connection information to the data warehouse
* Connection information for upstream data along with information
  which tables to pull (or not to pull)

There's a reasonable default so this might be as simple as this example:
```
{
    "s3": {
        "bucket_name": "<your bucket>"
    },
    "sources": [
        {
            "name": "dw"
        },
        {
            "name": "www",
            "read_access": "DATABASE_PRODUCTION",
            "include_tables": ["public.*"]
        }
    ]
}
```

Instead of passing a configuration file to every command, set
the configuration file once using an evironment variable:
```shell
DATA_WAREHOUSE_CONFIG=<path to config file>
export DATA_WAREHOUSE_CONFIG
```

#### Create the database in Redshift

Create the database either when starting the cluster in AWS console
or afterwards using:
```sql
CREATE DATABASE <dbname>;
```
(_Hint_: You can always connect to the `dev` database in your Redshift cluster.)


Set the environment variable that gives access as your cluster admin user:
```shell
DATA_WAREHOUSE_ADMIN=postgres://<admin user>:<password>@<host>:5439/<dbname>?sslmode=require
export DATA_WAREHOUSE_ADMIN
```

The environment variable `DATA_WAREHOUSE_ADMIN` is the default but can be
overridden in the configuration file.

#### Create groups, users and adjust privileges

Pick a password for the owner of schemas and tables.
Either add it on the command line or type it when prompted.
a [random password](https://xkcd.com/936/).)
```shell
python/scripts/initial_setup.py "horse_battery_staple"
```
(_Hint_: Running `initial_setup.py -h` without arguments will make a suggestion
for a random password.)

This creates the new owner / ETL user, the ETL group and end user group, and
the schemas, one per source.

(_Hint_: If you instead get an error message about a missing module "etl", then you
probably need to (re-)activate the virtual environment.)

Set the environment variable for DW access with the ETL user.
```shell
DATA_WAREHOUSE_ETL=postgres://<etl user>:<password>@<host>:5439/<dbname>?sslmode=require
export DATA_WAREHOUSE_ETL
```
And maybe store the password in your `~/.pgpass` file for good measure.

This is a good point to also create a user for yourself (or me) to test access:
```shell
python/scripts/create_user.py tom
```
Then type the password when prompted (or provide it on the command line if you
feel like nobody could be possibly watching running processes on your machine).

There are also options to add the user to the ETL group (which has r/w access) --
this is useful for users of ETL tools, e.g. our Spark-based ETL described below.
```shell
python/scripts/create_user.py --etl-user spark_etl
```

Finally, some developers might benefit from having a schema where they can
create tables.  See the options of `create_user.py`.

#### Download data into S3

You need to set whatever environment variables you have configured for access to
upstream databases. In the example above, that's `DATABASE_PRODUCTION`.

You can also either set environment variables for access to S3 or let the scripts
pick up information from `~/.aws/credentials` (which is where `aws configure`
will store your credentials).
```shell
AWS_ACCESS_KEY_ID= ...
AWS_SECRET_ACCESS_KEY= ...
export AWS_ACCESS_KEY_ID  AWS_SECRET_ACCESS_KEY
```

By default, `dump_to_s3.py` will download table designs to your local
`schemas` directory.  This may be a good place to start.
```shell
dump_to_s3.py
```

Once you created some table designs and added them to a repo,
adjust the path (here, my Git repositories are under `~/gits`):
```shell
dump_to_s3.py -s ~/gits/table-designs
```

If you have to interrupt the download, that's ok.  No files will be created twice.
This means, however, that you must clean out the data directory to retrieve newer
data or use the `-f` option.

If you need data for just one or a few tables, pass in a glob pattern.
```shell
dump_to_s3.py www.orders*
```
This will connect to only the `www` source database and download all tables
starting with `orders`.

#### Upload ETL views to S3 (CTAS)

For tables that are based on views, you need to update the definitions
separately.  These tables are refered to as *CTAS* after the DDL statement
that creates them (`CREATE TABLE ... AS SELECT ...`).

```shell
copy_to_s3.py -s ~/gits/analytics
```

#### Create tables and copy data from S3 into them

With the environment variables still set, run:
```shell
load_to_redshift.py
update_with_ctas.py
```

#### Hints

* Many scripts allow to preview steps using a `--dry-run` command line option.

* Also, you can often specify a specific source (meaning target schema).
```shell
load_to_redshift.py www
```

* And you can specify specific tables.  The scripts accept
  a ["glob" pattern](https://en.wikipedia.org/wiki/Glob_(programming))
  (also known as a shell pattern).  Examples:
```shell
load_to_redshift.py hyppo.sent_emails
update_with_ctas.py www.product*
```

* And don't forget that the prefix for S3 files is automatically picked on the
  user name, not based on your last data dump or upload. So you may have to
  specify the `--prefix` option.

* This allows to iterate quickly over changes before committing:
```shell
cd ~/gits/analytics
copy_to_s3.py -p wip analytics.dim_order &&
update_with_ctas.py -p wip analytics.dim_order
```

### Working on the table design

To optimize query performance, tables should be loaded with a defined
distribution style and sort keys. The design files should **not** live
within the data directory to make sure that the design files are not
accidentally deleted when deleting the data dumps. The design files
must be organized the same way that the data files are.

There are a number of useful queries in the
[Amazon Redshift Utilities](https://github.com/awslabs/amazon-redshift-utils)
to help with table designs.
