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

#### Additional packages

While our EC2 installations will use `requirements.txt` (see [bootstrap.sh](./bin/bootstrap.sh)),
you should always use `requirements-all.txt` for local development. The packages listed in that
file are installed when building the Docker image.

### Running unit tests and type checker

See the [Formatting code](https://github.com/harrystech/arthur-redshift-etl/blob/next/README.md#formatting-code)
section in the README file.

Keep this [cheat sheet](http://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html) close by for help with types etc.
