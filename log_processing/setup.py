from setuptools import find_packages, setup


setup(
    name="etl_log_processing",
    version="0.1.0",
    author="Harry's Data Engineering and Contributors",
    license="MIT",
    url="https://github.com/harrystech/arthur-redshift-etl",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "show_log_examples= etl_log_processing.parser:main",
            "log_search = etl_log_processing.search:main",
            "log_upload = etl_log_processing.upload:main"
        ]
    }
)
