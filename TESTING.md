# Running Unit Tests

## Inside the Docker Container

```shell
cd /opt/src/arthur-redshift-etl/
python3 -m unittest --verbose
```

## Inside a Virtual Environment

Install the Python code into a virtual environment and run the `unittest` module.

```shell
bin/build_virtual_env
source arthur_venv/bin/activate
python3 setup.py develop
python3 -m unittest --verbose
```

## Running With Coverage Reports

```shell
coverage3 run -m unittest --verbose
coverage3 report --skip-empty
coverage3 report --skip-empty --show-missing
```
