from setuptools import setup, find_packages


setup(
    name="redshift-etl",
    version="0.6.2",
    author="Harry's Data Engineering and Contributors",
    description="ETL code to ferry data from PostgreSQL databases to Redshift cluster",
    license="MIT",
    keywords="redshift etl",
    url="https://github.com/harrystech/harrys-redshift-etl",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=[
        "scripts/submit_arthur.sh"
    ],
    entry_points={
        # NB The script must end in ".py" so that spark submit accepts it as a Python script.
        'console_scripts': ['arthur.py=etl.commands:run_arg_as_command']
    }
)
