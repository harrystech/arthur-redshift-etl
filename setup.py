from setuptools import find_packages, setup


setup(
    name="redshift-etl",
    version="0.27.0",
    author="Harry's Data Engineering and Contributors",
    description="ETL code to ferry data from PostgreSQL databases (or S3 files) to Redshift cluster",
    license="MIT",
    keywords="redshift postgresql etl extract transform load",
    url="https://github.com/harrystech/harrys-redshift-etl",
    package_dir={"": "python"},
    packages=find_packages("python"),
    package_data={
        "etl": [
            "assets/*",
            "config/*"
        ]
    },
    scripts=[
        "python/scripts/submit_arthur.sh",
        "python/scripts/re_run_partial_pipeline.py"
    ],
    entry_points={
        "console_scripts": [
            # NB The script must end in ".py" so that spark submit accepts it as a Python script.
            "arthur.py = etl.commands:run_arg_as_command",
            "run_tests.py = etl.selftest:run_tests"
        ]
    }
)
