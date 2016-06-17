import os
from setuptools import setup, find_packages


setup(
    name="redshift-etl",
    version="0.5.0",
    author="Harry's Data Engineering and Contributors",
    description="ETL code to ferry data from PostgreSQL databases to Redshift cluster",
    license="MIT",
    keywords="redshift etl",
    url="https://github.com/harrystech/harrys-redshift-etl",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=[
        "scripts/initial_setup.py",
        "scripts/create_user.py",
        "scripts/dump_schemas_to_s3.py",
        "scripts/dump_data_to_s3.py",
        "scripts/load_to_redshift.py",
        "scripts/copy_to_s3.py",
        "scripts/update_in_redshift.py",
        "scripts/submit_local.sh"
    ]
)
