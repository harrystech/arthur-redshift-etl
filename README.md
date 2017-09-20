**ETL Code for Loading Data Into a Redshift-based Data Warehouse**

```
                _   _                  _____          _     _     _  __ _     ______ _______ _
     /\        | | | |                |  __ \        | |   | |   (_)/ _| |   |  ____|__   __| |
    /  \   _ __| |_| |__  _   _ _ __  | |__) |___  __| |___| |__  _| |_| |_  | |__     | |  | |
   / /\ \ | '__| __| '_ \| | | | '__| |  _  // _ \/ _` / __| '_ \| |  _| __| |  __|    | |  | |
  / ____ \| |  | |_| | | | |_| | |    | | \ \  __/ (_| \__ \ | | | | | | |_  | |____   | |  | |____
 /_/    \_\_|   \__|_| |_|\__,_|_|    |_|  \_\___|\__,_|___/_| |_|_|_|  \__| |______|  |_|  |______|
```

Arthur is an ETL tool for managing a data warehouse in the AWS ecosystem. Arthur is designed to manage a warehouse in full-rebuild mode where the entire warehouse is rebuilt, from scratch, every night. Arthur is *not* designed to support streaming or micro-batch ETL. Arthur is best suited for organizations whose data are managed in a stateful transactional database and have lots of complicated business logic for their data transformations that they want to be able to manage effectively.

If you’re interested in this approach or are in a similar situation, then we’d like to talk to you.  Please reach out and let’s have a data & analytics meetup.

This README outlines how to get started with the ETL.
This includes information about setting up _Arthur_ which is the driver for ETL activities.

You are probably (also) looking for the [Wiki](https://github.com/harrystech/arthur-redshift-etl/wiki) pages,
which include a lot more information about the ETL and what it does (and why it does what it does).
And if something appears amiss, check out the [issues page](https://github.com/harrystech/arthur-redshift-etl/issues).

# Installing the code

See the separate [INSTALL.md](./INSTALL.md) file.


# Configuring the ETL (and upstream sources)

The best approach is to have a separate repo for your data warehouse that contains the configuration files
and all the table design files and transformations.  The documentation will in many places assume
that you have a "*sibling*" repo so that when within the repo for your local data warehouse (with
configuration, credentials, and table designs), you can simply use `../arthur-redshift-etl/` to find
your way back to this ETL code.

## Redshift cluster and users

Although the Redshift cluster can be administered using the AWS console and `psql`, some
helper scripts will make setting up the cluster consistently much easier.
(See below for `initialize` and `create_user`.)

Also, add the AWS IAM role that the database owner may assume within Redshift
to your settings file so that Redshift has the needed permissions to access the
folder in S3.
(And don't forget to add the role to the list of known IAM roles in Redshift.)

## Sources

See the [Wiki](https://github.com/harrystech/arthur-redshift-etl/wiki) pages about
a description of configurations.

# Running the ETL (`arthur.py`)

## General notes about the CLI

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

## Prerequisites for running the ETL in a cluster

### Creating a credentials file

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

### Copying code into the S3 bucket

Copy the ETL code (including bootstrap scripts and configuration):
```shell
export DATA_WAREHOUSE_CONFIG="<path to directory with config files and credentials>"
# export DATA_WAREHOUSE_CONFIG="\cd ./config && \pwd`
bin/upload_env.sh "<your S3 bucket>" $USER
```

### Starting a cluster and submitting commands

Start a cluster:
```shell
bin/launch_emr_cluster.sh
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

## Initializing the Redshift cluster

| Sub-command   | Goal |
| ---- | ---- |
| `initialize`  | Create schemas, groups and users |
| `create_user`    | Create (or configure) users that are not mentioned in the configuration file |

```shell
# The commands to setup the data warehouse users and groups or any database is by ADMIN (connected to `dev`)
arthur.py initialize  # NOP but errors out
arthur.py initialize development --with-user-creation  # Must create users and groups on first call
```

## Starting with design files (and managing them)

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

## Loading and updating data

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

Note that when a `load` fails, the work until the failed relation is still in the "staging" schemas. You can continue
the load after fixing any query errors or input data, using:
```
arthur.py upgrade --with-staging-schemas --continue-from failed_relation.in_load_step
```

## Dealing with schemas (create, restore)

| Sub-command   | Goal |
| ---- | ---- |
| `create_schemas`  | Create schemas; normally `load` will do that for you |
| `promote_schemas`  | Bring back schemas from backup if `load` was aborted or promote staging after fixing any issues |

To test permisions (granting and revoking), use for any schema:
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

The above also works when we're just interested in transformations. This can
save time while iterating on transformations since the sources won't be reloaded.

Example:
```shell
arthur.py show_downstream_dependents -q | grep -v 'kind=DATA' | tee transformations.txt

arthur.py sync @transformations.txt
arthur.py upgrade --only @transformations.txt
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
| `render_template` | Return a JSON document that has values like `${resources.vpc.id}` filled in |
| `show_value` | Show value of a single variable based on current configuration|
| `show_vars` | Show variables and their values based on current configuration |

Note this leaves references like `#{parameter}` used by AWS tools in place.

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
arthur.py initialize staging --dry-run  # In case you want to see what happeens but not lose all schemas.
```

Once everything is working fine in staging, you can promote the code into production.
```shell
./bin/sync_env.sh "<your S3 bucket>" staging production
```

# Contributing and Releases

## Creating pull requests

Pull requests are welcome!

Development takes place on the `next` branch. So go ahead, and create a branch off `next` and work
on the next ETL feature.

* Please run code through [pep8](https://www.python.org/dev/peps/pep-0008/) (see [local config](.pep8)):
```shell
pep8 python
```

* Even better, set up the git pre-commit hook to prevent you from accidentally breaking convention
```shell
ln -s -f ../../githooks/pre-commit ./.git/hooks/pre-commit
```

* Run the unit tests (including doc tests) and type checker before submitting a PR.
```
run_tests.py
```

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

* Create a new branch for the release candidate, e.g. `v0_22_0` for v0.22.0.
(Do yourself a favor and use underscores in branch names and periods in the tag names.)

* Your first commit on this branch is a bump in the version number in `setup.py`.

* Create a pull request for your new branch.

* Go through pull requests that are ready for release and change their base branch to your release branch (in our example, `v0_22_0`).
    * Make sure the PR message contains the Issue number or the Jira story (like DW-99).
    * Add the changes from the story work into comments of the PR.
        * Consider changes that are user facing and make there's a summary for those
        * List all bug fixes (especially when there are associated tickets)
        * Highlight internal changes, like changes in data structures or added control flow
    * Then merge the ready PRs into your release candidate.

* Test then merge the PR with your release candidate into `next`.

* Create a release under [Releases](https://github.com/harrystech/arthur-redshift-etl/releases).
    * Create a new release and set the release version, e.g. `v1.2.0`.
    * Copy the comments from the PR where you collected all the changes into the release notes.
    * Save the release which will add the tag of the release.

* Ship the new version using `upload_env.sh` in development.

### Releasing code to master branch

Once code is considered ready for production:
* Merge `next` into `master`
```
git checkout master
git pull
git merge origin/next
git push
```
* Then merge `master` back into `next` to ensure any hotfixes on master get picked up:
```
git checkout next
git pull
git merge --no-ff origin/master
git push
```

# Tips & Tricks

## Miscellaneous

### Using command completion in the shell

For the bash shell, there is a file to add command completion that allows to tab-complete schemas and table names.
```shell
source etc/arthur_completion.sh
```

If you are normally in the repo for your data warehouse configuration, then this might be better:
```shell
source ../arthur-redshift-etl/etc/arthur_completion.sh
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

When you pull a new version of Arthur, you should re-install the ETL code.
This is especially important to pick up changes in the scripts!
```shell
pip3 install -r requirements.txt
python3 setup.py develop
```
