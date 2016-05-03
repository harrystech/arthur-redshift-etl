# Using the Python library and scripts

## Installation and usage

The library and scripts leverage:
* [Python3](https://docs.python.org/3/) so use `pip3` and `python3`
* [Psycopg2](http://initd.org/psycopg/docs/) to connect to PostgreSQL
* [boto3](https://boto3.readthedocs.org/en/latest/) to interact with S3
* [PyYAML](http://pyyaml.org/wiki/PyYAML) for configuration files

In order to use this code, create a virtual environment and install the Python code.

**Note** this assumes you are in the *top-level* directrory of the Redshift ETL.
```shell
mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade pip
pip3 install -r python/requirements.txt
(cd python && python3 setup.py develop)
```

### Hints

* Don't worry if you get assertion violations while building a wheel for PyYAML.

## Developing

Pull requests are welcome!

* Please run code through [pep8](https://www.python.org/dev/peps/pep-0008/) (see [local config](.pep8)):
```shell
pep8 python
```

* Consider installing [iPython](https://ipython.org/index.html).
```shell
pip3 install ipython
```
