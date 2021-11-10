"""Provide "self-test" feature of Arthur, which executes all doctests."""

import doctest
import pathlib
import sys
import unittest

import pkg_resources

# Skip etl.commands to avoid circular dependency.
# TODO(tom): There is a bug: doctests from etl.commands are not run with "python3 -m etl.selftest".
import etl.config
import etl.config.env
import etl.config.settings
import etl.data_warehouse
import etl.db
import etl.design
import etl.design.bootstrap
import etl.design.load
import etl.dialect.redshift
import etl.errors
import etl.explain
import etl.extract
import etl.file_sets
import etl.json_encoder
import etl.load
import etl.logs.cloudwatch
import etl.logs.formatter
import etl.monitor
import etl.names
import etl.pipeline
import etl.relation
import etl.s3
import etl.sync
import etl.templates
import etl.text
import etl.unload
import etl.util
import etl.util.timer
import etl.validate


def load_tests(loader, tests, pattern):
    """
    Add tests from doctests and discover unittests in "tests" directory.

    See https://docs.python.org/3.5/library/unittest.html#load-tests-protocol
    """
    etl_modules = sorted(mod for mod in sys.modules if mod.startswith("etl"))
    for mod in etl_modules:
        test_suite = doctest.DocTestSuite(mod)
        if test_suite.countTestCases():
            print(f"Adding {test_suite.countTestCases()} doctest(s) from '{mod}'")
            tests.addTests(test_suite)
    # This assumes that "tests" is parallel to "python" directory.
    etl_path = pathlib.PosixPath(pkg_resources.resource_filename("etl", "etl"))
    start_dir = etl_path.parents[2].joinpath("tests")
    if not start_dir.exists():
        print(f"Skipping unittests (directory not found: '{start_dir}')", flush=True)
        return tests

    test_suite = loader.discover(start_dir)
    print(f"Adding {test_suite.countTestCases()} unittests from '{start_dir}' directory", flush=True)
    tests.addTests(test_suite)
    return tests


def run_selftest(module_: str, log_level: str = "INFO") -> None:
    # Translate log levels into numeric levels needed by unittest.
    verbosity_levels = {"DEBUG": 2, "INFO": 1, "WARNING": 0, "CRITICAL": 0}
    verbosity = verbosity_levels.get(log_level, 1)

    print("Running doctests...", flush=True)
    test_runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=verbosity)
    test_program = unittest.main(
        module=module_, exit=False, testRunner=test_runner, verbosity=verbosity, argv=sys.argv[:2]
    )
    test_result = test_program.result
    if not test_result.wasSuccessful():
        raise etl.errors.SelfTestError(
            "Unsuccessful (run=%d, errors=%d, failures=%d)"
            % (test_result.testsRun, len(test_result.errors), len(test_result.failures))
        )


if __name__ == "__main__":
    try:
        run_selftest(__name__)
    except Exception as exc:
        print(exc)
        sys.exit(1)
