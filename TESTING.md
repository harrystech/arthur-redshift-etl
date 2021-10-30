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
