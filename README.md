[![Lint Python code](https://github.com/harrystech/arthur-redshift-etl/workflows/Lint%20Python%20code/badge.svg)](https://github.com/harrystech/arthur-redshift-etl/actions?query=workflow%3A%22Lint+Python+code%22)
[![Unit Tests](https://github.com/harrystech/arthur-redshift-etl/workflows/Unit%20Tests/badge.svg)](https://github.com/harrystech/arthur-redshift-etl/actions?query=workflow%3A%22Unit+Tests%22)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Language grade: JavaScript](https://img.shields.io/lgtm/grade/javascript/g/harrystech/arthur-redshift-etl.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/harrystech/arthur-redshift-etl/context:javascript)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/harrystech/arthur-redshift-etl.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/harrystech/arthur-redshift-etl/context:python)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/harrystech/arthur-redshift-etl.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/harrystech/arthur-redshift-etl/alerts/)

**ETL Code for Loading Data Into a Redshift-based Data Warehouse**
---

```
                _   _                  _____          _     _     _  __ _     ______ _______ _
     /\        | | | |                |  __ \        | |   | |   (_)/ _| |   |  ____|__   __| |
    /  \   _ __| |_| |__  _   _ _ __  | |__) |___  __| |___| |__  _| |_| |_  | |__     | |  | |
   / /\ \ | '__| __| '_ \| | | | '__| |  _  // _ \/ _` / __| '_ \| |  _| __| |  __|    | |  | |
  / ____ \| |  | |_| | | | |_| | |    | | \ \  __/ (_| \__ \ | | | | | | |_  | |____   | |  | |____
 /_/    \_\_|   \__|_| |_|\__,_|_|    |_|  \_\___|\__,_|___/_| |_|_|_|  \__| |______|  |_|  |______|
```

Arthur is an ETL tool for managing a data warehouse in the AWS ecosystem.
Arthur is designed to manage a warehouse in full-rebuild mode where the entire warehouse is rebuilt,
from scratch, every night. Arthur is not designed to support streaming or micro-batch ETLs.
Arthur is best suited for organizations whose data are managed in a stateful transactional database and have
lots of complicated business logic for their data transformations that they want to be able to manage effectively.

_If you’re interested in this approach or are in a similar situation, then we’d like to talk to you._
_Please reach out and let’s have a data & analytics meetup._

This README outlines how to get started with the ETL'ing and basic principles.
This includes information about setting up _Arthur_ which is the driver for ETL activities.

You are probably (also) looking for the [wiki pages](https://github.com/harrystech/arthur-redshift-etl/wiki),
which include a lot more information about the ETL and what it does (and why it does what it does).
And if something appears amiss, check out the [issues page](https://github.com/harrystech/arthur-redshift-etl/issues).

# Installing the code

See the separate [INSTALL.md](./INSTALL.md) file.

# Documentation

See also our [wiki pages](https://github.com/harrystech/arthur-redshift-etl/wiki).
And here's a [presentation about Arthur](https://www.youtube.com/watch?v=iVL-lNEKOm4),
given at the Startup booth during the AWS Summit in New York.

# Configuring the ETL (and upstream sources)

The best approach is to have a separate repo for your data warehouse that contains
the configuration files and all the table design files and transformation code in SQL.
The documentation will in many places assume that you have a "*sibling*" repo so that when within the repo for your
local data warehouse (with configuration, credentials, and table designs),
you can simply use `../arthur-redshift-etl/` to find your way back to this ETL code.

## Redshift cluster and users

Although the Redshift cluster can be administered using the AWS console and `psql`, some
helper scripts will make setting up the cluster consistently much easier.
(See below for `initialize` and `create_user`.)

Also, add the AWS IAM role that the database owner may assume within Redshift
to your settings file so that Redshift has the needed permissions to access the
folder in S3.
(And don't forget to add the role to the list of known IAM roles in Redshift.)

## Sources

See the [wiki pages](https://github.com/harrystech/arthur-redshift-etl/wiki) about
a description of configurations.

# Running the ETL (`arthur.py`)

## General notes about the CLI

* It's easiest to use a Docker container to run `arthur.py`.
* Commands will provide usage information when you use `-h`.
    - There is also a `help` command that provides introductions to various topics.
* Commands need configuration files. They will pick up all files in a local `./config` directory
    or from whatever directory to which `DATA_WAREHOUSE_CONFIG` points.
* Commands accept a `--dry-run` command line flag to test without modifying the environment.
* Most commands allow the use of glob patterns to select specific schema(s) or table(s).
* Most commands use `--prefix` to select a folder in the S3 bucket.
     - A few development commands normally pick up local files first and you need to add `--remote` to go to S3.
* To pick a prefix without specifying it every time, set the environment variable `ARTHUR_DEFAULT_PREFIX`.
* Log files are by default in `arthur.log`.  They are rotated and deleted so that your disk doesn't fill up too much.
* Logs that are collected from data pipelines are in `stderr.gz` or `StdErr.gz` files since _Arthur_ logs to stderr.
* To see more log information, use `--verbose`. To see less, use `--quiet`.
* To see them formatted in the console the same way as they are formatted in the log files, use `--prolix`.
* You could copy data manually, but you probably shouldn't and let `arthur.py sync` manage files.
* You can use environment variables to pass in credentials for database access, but you should use a file for that.

## Prerequisites for running the ETL in a cluster

### Creating a credentials file

All credentials can be picked up from environment variables by the ETL. Instead of setting these
variables before starting the ETL, you can also add a file with credentials to the `config` directory
where the ETL will pick them up for you. The credentials file should be formatted just like a shell
file would be to set variables, meaning lines should have the form:
```
# Lines with '#' are ignored.
NAME=value
# Although not meaningful within the ETL code, you can use the "export" syntax from Bash
export NAME=value
```

The minimal credentials file contains the login information for the ETL user that _Arthur_ will use
to execute in Redshift.  Make sure this file exists in your data warehouse repo as `config/credentials.sh`:
```
DATA_WAREHOUSE_ETL=postgres://etl:<password>@<host>:<port>/<dbname>?sslmode=require
```

If you need to make changes in the cluster beyond schema changes, you will also need an admin:
```
DATA_WAREHOUSE_ADMIN=postgres://admin:<password>@<host>:<port>/<dbname>?sslmode=require
```

### Starting Arthur in a Docker container

The [INSTALL.md](INSTALL.md) file will explain how to setup a Docker image to run _Arthur_.

Once you have that, getting to a prompt is easy:
```shell
bin/run_arthur.sh ../warehouse-repo/config production
```

This command will set the path to the configuration files and default environment (_a.k.a._ prefix) for you.

### Copying code into the S3 bucket

From within the Docker container, use:
```shell
upload_env.sh
```

For this to work, you have to set the `object_store` in one of your configuration files.

As an alternative, you can use `bin/deploy_arthur.sh` from outside a container.

### Starting a cluster and submitting commands

Start a cluster:
```shell
launch_emr_cluster.sh
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

Don't worry -- the script `launch_emr_cluster.sh` will show this information before it exits.

Don't forget to run `terminate_emr_cluster.sh` when you're done.

## Initializing the Redshift cluster

| Sub-command   | Goal |
| ---- | ---- |
| `initialize`  | Create schemas, groups and users |
| `create_user`    | Create (or configure) users that are not mentioned in the configuration file |

```shell
# The commands to setup the data warehouse users and groups or any database is by ADMIN (connected to `dev`)
arthur.py initialize
arthur.py initialize development --with-user-creation  # Must create users and groups on first call
```

## Starting with design files (and managing them)

| Sub-command   | Goal |
| ---- | ---- |
| `bootstrap_sources`  | Download schemas from upstream sources and bootstrap design files |
| `bootstrap_transformations`  | Bootstrap (or update) design files for transformations based on new SQL queries |
| `explain`  | Review query plan for transformations |
| `validate`  | After making changes to the design files, validate that changes are consistent with the expected format and with respect to each other |
| `sync` | Upload your local files to your data lake |

```shell
# This will upload local files related to one schema into your folder inside the S3 bucket:
arthur.py sync "<schema>"
# This will upload local files for just one table
arthur.py sync "<schema>.<table>"
```

Note that when running sync that involved changes of source schemas or configurations, you must use:
```shell
arthur.py sync --force --deploy "<schema>.<table>"
```

### Deploying into production

We prefer to have a short and succinct way to deploy our data warehouse files (configuration, design files and
transformations) into production. So instead of starting a bash and running `sync`, just do:

```shell
bin/deploy_with_arthur.sh -p aws-prod-profile ../repo/config_directory/ production
```

## Loading and updating data

| Sub-command   | Goal |
| ---- | ---- |
| `extract`  | Get data from upstream sources (databases or S3) |
| `load`, `upgrade` | Make data warehouse "structural" changes and let data percolate through |
| `update` | Move data from upstream sources and let it percolate through |
| `unload` | Take data from a relation in the data warehouse and extract as CSVs into S3 |

```shell
arthur.py extract
arthur.py load  # This will automatically create schemas and tables as necessary
```

Note that when a `load` fails, the work until the failed relation is still in the "staging" schemas. You can continue
the load after fixing any query errors or input data, using:
```
arthur.py upgrade --with-staging-schemas --continue-from failed_relation.in_load_step
```

Within a production environment, check out `install_pizza_pipeline.sh` which provides a consistent interface.

## Dealing with schemas (create, restore)

| Sub-command   | Goal |
| ---- | ---- |
| `create_schemas`  | Create schemas; normally `load` will do that for you |
| `promote_schemas`  | Bring back schemas from backup if `load` was aborted or promote staging after fixing any issues |

To test permissions (granting and revoking), use this for any schema:
```
arthur.py create_schemas schema_name
arthur.py create_schemas --with-staging schema_name
arthur.py promote_schemas --from staging schema_name
```

## Working with subsets of tables

| Sub-command   | Goal |
| ---- | ---- |
| `show_downstream_dependents`  | Inspect the other relations impacted by changes to the selected ones |
| `show_upstream_dependencies`  | Inspect which other relations feed the selected ones |

The patterns used by commands like `extract` or `load` may be provided using files.
Together with `show_downstream_dependents` and `show_upstream_dependencies`, this opens up
opportunities to work on a "sub-tree" of the data warehouse.

### Working with just the source schemas or transformation schemas

At the beginning it might be worthwhile to focus just on tables in source schemas -- those
tables that get loaded using CSV files after `extract`.
```shell
arthur.py show_downstream_dependents -q | grep 'kind=DATA' | tee sources.txt

# List CSV files and manifests, then continue with upgrade etc.
arthur.py ls --remote @sources.txt
```

Note that you should use the special value `:transformations` when you're interested
to work with transformations.

Example:
```shell
arthur.py show_downstream_dependents -q --continue-from=:transformations | tee transformations.txt

arthur.py sync @transformations.txt
arthur.py upgrade --only @transformations.txt

# If you don't make changes to transformations.txt, then you might as well use
arthur.py upgrade --continue-from=:transformations
```

### Working with a table and everything feeding it

While working on transformations or constraints, it might be useful to focus on just a
set of of tables that feed data into it.

Example:
```shell
arthur.py show_upstream_dependencies -q www.users | tee www_users.txt

arthur.py sync www.users
arthur.py upgrade --only @www_users.txt
```

## Using configuration values to fill in templates

AWS service templates can be filled out based on configuration in the ETL.

| Sub-command   | Goal |
| ---- | ---- |
| `render_template` | Return a JSON document that has values (like `${resources.vpc.id}`) filled in |
| `show_value` | Show value of a single variable based on current configuration |
| `show_vars` | Show variables and their values based on current configuration |

Note this leaves references like `#{parameter}`, which are used by AWS tools, in place.

Example:
```shell
arthur.py render_template ec2_instance
arthur.py show_value object_store.s3.bucket_name
arthur.py show_vars object_store.s3.*
```

# Working with a staging environment

A staging environment can help with deploying data that you'll be confident to release into production.

```shell
arthur.py initialize staging
arthur.py initialize staging --dry-run  # In case you want to see what happens but not lose all schemas.
```

Once everything is working fine in staging, you can promote the code into production.
```shell
sync_env.sh "<your S3 bucket>" staging production
```

Don't forget to upload any `credentials_*.sh` or `environment.sh` files as needed for production.

# Contributing and Releases

## Creating pull requests

Pull requests are welcome!

Development takes place on the `next` branch. So go ahead, and create a branch off `next` and work
on the next ETL feature.

### Formatting code

Please format your code using:
* [black](https://github.com/psf/black)
* [isort](https://github.com/timothycrosley/isort)

Also, we want to run code through:
* [flake8](https://flake8.pycqa.org/en/latest/)
* [mypy](http://mypy-lang.org/)

#### Adding a pre-commit hook

Use `pre-commit` to run linters automatically on commit.
This is the preferred method of running formatters and linters.

```shell
brew bundle
pre-commit install
```

You can also run the linters directly:
```shell
pre-commit run
```

#### Installing linters locally

To use the linters (`isort`, `black`, `flake8`, `mypy`) locally, install them using:
```
python3 -m venv arthur_venv
source arthur_venv/bin/activate
python3 -m pip install --upgrade pip==20.3.4
python3 -m pip install --requirement requirements-linters.txt
```

##### Running formatters locally

```shell
black python/ setup.py
isort python/ setup.py
```

##### Running linters locally

```shell
black --check python/ setup.py
isort --check-only python/ setup.py
flake8 python setup.py
mypy python
```

### References

* Please have meaningful comments and git commit messages
(See [Chris's blog](http://chris.beams.io/posts/git-commit/))

* Use git rebasing to merge your commits into logical chunks
(See [Thoughtbot's guidelines](https://github.com/thoughtbot/guides/blob/master/protocol/git/README.md))

## Releasing new versions

Here are the basic steps to release a new version. Appropriate as you deem appropriate.

### Creating patches

For minor updates, use a PR to update `next`. Anything that requires integration tests or
running code in development cluster first should go through a release candidate.

### Creating a release candidate

* Create a new branch for the release candidate, e.g. `v1_2_0` for v1.2.0.
(Do yourself a favor and use underscores in branch names and periods in the tag names.)

* Your first commit on this branch is a bump in the version number in `setup.py`.

* Create a pull request for this new branch, add a message to state that it is <pre>"Release Candidate for v1.2.0"</pre>.

* Go through open pull requests that are ready for release and change their base branch to your release branch (in our example, `v1_2_0`).
    * Make sure the PR message contains the Issue number or the Jira story (like DW-99) or the Issue number from this repo, as applicable.
    * Add the changes from the story work into comments of the PR.
        * Consider changes that are user facing and make sure there's a summary for those
        * List all bug fixes (especially when there are associated tickets)
        * Highlight internal changes, like changes in data structures or added control flow
    * Then merge the ready PRs into your release candidate.

* After testing, merge the PR with your release candidate into `next`.

* Create a release under [Releases](https://github.com/harrystech/arthur-redshift-etl/releases).
    * Create a new release and set the release version, e.g. `v1.2.0`.
    * Copy the comments from the PR where you collected all the changes into the release notes.
    * Save the release which will add the tag of the release.

* Ship the new version using `upload_env.sh` in development.  Wait at least a day before promoting to production.

### Releasing code to master branch

Once code is considered ready for production (and you've made sure there's an **updated** version number in `setup.py`):
* Merge `next` into `master`
```
git checkout master
git pull
git merge --no-ff origin/next
git push
```
* Tag the latest commit on master
```
git tag # pick a tag following SemVer
git push origin --tags
```
* Then merge `master` back into `next` to ensure any hotfixes on master get picked up:
```
git checkout next
git pull
git merge origin/master
git push
```

# Tips & Tricks

## Miscellaneous

### Using command completion in the shell

For the bash shell, there is a file to add command completion that allows to tab-complete schemas and table names.
```shell
source etc/arthur_completion.sh
```
(Within a Docker container, that happens automatically.)

### EMR login / EC2 login

You can use the `.ssh/config` file to pick the correct user (`hadoop`) for the cluster and to automatically
pick up a key file.  Replace the `IdentityFile` value with the location of your key pair file.
```
Host ec2-*.amazonaws.com
  ServerAliveInterval 60
  User hadoop
  IdentityFile ~/.ssh/dw-dev-keypair.pem
```

If you find yourself using a one-off EC2 instance more often than an EMR cluster, change the `User`:
```
  User ec2-user
```

## Virtual environment

If you want to have a virtual environment in your local directory, _e.g._ to make working with an IDE easier,
then these steps will work:
```shell
python3 -m venv arthur_venv
source arthur_venv/bin/activate
python3 -m pip install --requirement requirements-dev.txt
```
