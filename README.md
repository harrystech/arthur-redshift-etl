# harrys-redshift-etl

ETL Code for Loading Data Into Redshift-based Data Warehouse

## Overview

See [ETL Flow diagram](doc/etl_flow.svg) for a basic overview.  The idea
is that there are upstream PostgreSQL databases which should be collected
into a single data warehouse where their data is made available to BI users.

The tools in this repo fall into two categories:
    1. Prototyping code to bring up the warehouse and experiment with table designs.
    2. Production code to bring over data (on, say, a daily basis)

Either tool set expects that
* data sits in S3 between dump and load,
* authentication information is set using environment variables, and
* a configuration file describes upstream sources.

## Working with the Python ETL code to bring up a data warehouse

### Creating the protozoic / prototypical data warehouse

**Note** that you'll have to have your Python environment set up,
see [Python README](python/README.md).

Make sure the virtual environment is active:
```shell
source venv/bin/activate
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
CREATE DATABASE 'db';
```
(_Hint_: You can always connect to the `dev` database.)


Set the environment variable that gives access as your cluster admin user:
```shell
DATA_WAREHOUSE_ADMIN=postgres://<admin user>:<password>@<host>:5439/<dbname>?sslmode=require
export DATA_WAREHOUSE_ADMIN
```

The environment variable `DATA_WAREHOUSE_ADMIN` is the default but can be
overriden in the configuration file.

#### Create groups, users and adjust privileges

Pick a password for the ETL user.  Either add it on the command line or type
it when prompted.
(_Hint_: Running `initial_setup.py -h` without arguments will make a suggestion for
a [random password](https://xkcd.com/936/).)
```shell
python/scripts/initial_setup.py "horse_battery_staple"
```

This creates the new ETL user, the ETL group and end user group, and
fiddles with permissions for schemas.
(If you instead get an error message about a missing module "etl", then you
probably need to (re-)activate the virtual environment.)

Set the environment variable for DW access with the ETL user.  (We will not
continue with the cluster admin from here on).
```shell
DATA_WAREHOUSE=postgres://<etl user>:<password>@<host>:5439/<dbname>?sslmode=require
export DATA_WAREHOUSE
```
(And store the password in your `~/.pgpass` file for good measure.)

#### Download data into S3

Set whatever environment variables you have configured for access to upstream
databases.

You can also either set environment variables for access to S3 or let the scripts
pick up information from `~/.aws/credentials` (which is where `aws configure`
will store credentials).
```shell
AWS_ACCESS_KEY_ID= ...
AWS_SECRET_ACCESS_KEY= ...
export AWS_ACCESS_KEY_ID  AWS_SECRET_ACCESS_KEY
```

Now go for it:
```shell
python/scripts/dump_to_s3.py
```

If you have to interrupt the download, go for it.  No files will be created twice.  (This means,
however, that you must clean out the data directory to retrieve newer data or use
the `-f` option.)

#### Upload ETL scripts to S3

Upload the definitions of the analytics tables directly.  Adjust the path in
the example below to match your setup.  (My Git repositories are all under `~/gits`.)
```shell
python/scripts/copy_to_s3.py ~/gits/analytics
```

#### Create tables and copy data from S3 into them

With the environment variables still set, run:
```shell
python/scripts/load_to_redshift.py
python/scripts/update_with_ctas.py
```

#### Hints

* Many scripts allow to preview steps using a `--dry-run` command line option.
  Some will also allow you to keep running even in presence of errors,
  look for `--keep-going`.

* Also, you can often specify a specific source (meaning target schema).
```shell
python/scripts/load_to_redshift.py zoo_tables.*
```

* And you can specify specific tables.  The scripts accept
  a ["glob" pattern](https://en.wikipedia.org/wiki/Glob_(programming))
  (also known as shell patterns).  Examples:
```shell
python/scripts/load_to_redshift.py hyppo.sent_emails
python/scripts/update_with_ctas.py *.viewable_product*
```

* And don't forget that the prefix is automatically picked on the current date, not
  based on your last dump or upload. So you may have to specify the `--prefix` option.

* But the prefix doesn't have to be a date.  To test changes on some query, you can
  also set the prefix to, say, `wip`. Example work flow to work on the `order_margins` query,
  where you will have to replace the directory of Git repos (in my example: `~/gits/`):
```shell
( cd ~/gits/harrys-redshift-etl &&
  source venv/bin/activate &&
  python/scripts/copy_to_s3.py -p wip -s ~/gits/analytics analytics.order_margins &&
  python/scripts/update_with_ctas.py -p wip analytics.order_margins )
```

### Working on the table design

To optimize query performance, tables should be loaded with a defined
distribution style and sort keys. You can write table descriptions that
will replace the `.ddl` files when creating tables in Redshift.  In order
to leverage them, specify their location with the `load_to_redshift.py` script.

The design files should **not** live within the data directory to make sure
that the design files are not accidentally deleted when deleting the data
dumps.  The design files must be organized the same way that the data files are.
