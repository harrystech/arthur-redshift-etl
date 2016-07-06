from setuptools import setup, find_packages


setup(
    name="redshift-etl",
    version="0.6.0",
    author="Harry's Data Engineering and Contributors",
    description="ETL code to ferry data from PostgreSQL databases to Redshift cluster",
    license="MIT",
    keywords="redshift etl",
    url="https://github.com/harrystech/harrys-redshift-etl",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=[
        "scripts/submit_local.sh"
    ],
    entry_points={
        'console_scripts': ['arthur=etl.commands:run_arg_as_command']
    }
)
