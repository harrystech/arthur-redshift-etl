import os
import os.path
from setuptools import setup, find_packages


setup(
    name="redshift-etl",
    version="0.2",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=[
        "scripts/copy_to_s3.py",
        "scripts/create_user.py",
        "scripts/dump_to_s3.py",
        "scripts/initial_setup.py",
        "scripts/load_to_redshift.py",
        "scripts/split_csv.py",
        "scripts/update_with_ctas.py",
        "baseline/modified_rows.py"
    ]
)
