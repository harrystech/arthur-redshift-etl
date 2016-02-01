from setuptools import setup, find_packages


setup(
    name="HarrysRedshiftETL",
    version="0.1",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=['scripts/create_user.py', 'scripts/initial_setup.py']
)
