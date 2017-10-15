from setuptools import find_packages, setup


setup(
    name="etl_log_processing",
    version="1.1.0",
    author="Harry's Data Engineering and Contributors",
    license="MIT",
    url="https://github.com/harrystech/arthur-redshift-etl",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "show_log_examples= etl_log_processing.parser:main",
            "search_log = etl_log_processing.search:main",
            "upload_log = etl_log_processing.upload:main"
        ]
    }
)
