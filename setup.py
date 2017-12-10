from setuptools import find_packages, setup


setup(
    name="redshift_etl",
    version="1.6.0",
    author="Harry's Data Engineering and Analytics Engineering",
    description="ETL code to ferry data from PostgreSQL databases or S3 files to Redshift clusters",
    license="MIT",
    keywords="redshift postgresql ETL ELT extract transform load",
    url="https://github.com/harrystech/arthur-redshift-etl",
    package_dir={"": "python"},
    packages=find_packages("python"),
    package_data={
        "etl": [
            "assets/*",
            "config/*",
            "templates/*"
        ]
    },
    scripts=[
        "python/scripts/install_pizza_load_pipeline.sh",
        "python/scripts/install_rebuild_pipeline.sh",
        "python/scripts/install_refresh_pipeline.sh",
        "python/scripts/install_validation_pipeline.sh",
        "python/scripts/launch_ec2_instance.sh",
        "python/scripts/launch_emr_cluster.sh",
        "python/scripts/re_run_partial_pipeline.py",
        "python/scripts/sns_subscribe.sh",
        "python/scripts/submit_arthur.sh"
    ],
    entry_points={
        "console_scripts": [
            # NB The script must end in ".py" so that spark submit accepts it as a Python script.
            "arthur.py = etl.commands:run_arg_as_command",
            "run_tests.py = etl.selftest:run_tests"
        ]
    }
)
