from setuptools import setup, find_packages


setup(
    name="redshift-etl",
    version="0.2",
    packages=find_packages(),
    package_data={'etl': ["config/*"]},
    scripts=['scripts/create_user.py', 'scripts/initial_setup.py']
)
