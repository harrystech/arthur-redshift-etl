from setuptools import find_packages, setup

ARTHUR_VERSION = "1.47.0"


setup(
    author="Harry's Data Engineering and Analytics Engineering",
    description="ETL code to ferry data from PostgreSQL databases or S3 files to Redshift clusters",
    entry_points={
        "console_scripts": [
            # TODO(tom): Remove .py extension.
            "arthur.py = etl.commands:run_arg_as_command",
        ]
    },
    keywords="redshift postgresql ETL ELT extract transform load",
    license="MIT",
    name="redshift_etl",
    package_dir={"": "python"},
    packages=find_packages("python"),
    package_data={"etl": ["assets/*", "config/*", "logs/*.json", "templates/sql/*", "templates/text/*"]},
    python_requires=">=3.6, <4",
    scripts=[
        "python/scripts/compare_events.py",
        "python/scripts/install_extraction_pipeline.sh",
        "python/scripts/install_pizza_load_pipeline.sh",
        "python/scripts/install_pizza_pipeline.sh",
        "python/scripts/install_rebuild_pipeline.sh",
        "python/scripts/install_refresh_pipeline.sh",
        "python/scripts/install_upgrade_pipeline.sh",
        "python/scripts/install_validation_pipeline.sh",
        "python/scripts/launch_ec2_instance.sh",
        "python/scripts/launch_emr_cluster.sh",
        "python/scripts/re_run_partial_pipeline.py",
        "python/scripts/sns_subscribe.sh",
        "python/scripts/submit_arthur.sh",
        "python/scripts/terminate_emr_cluster.sh",
    ],
    url="https://github.com/harrystech/arthur-redshift-etl",
    version=ARTHUR_VERSION,
    zip_safe=False,
)
