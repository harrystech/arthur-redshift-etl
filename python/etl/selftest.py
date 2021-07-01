"""Provide "self-test" feature of Arthur, which executes all doctests."""

import doctest
import logging
import sys
import unittest
from typing import Optional

# Skip etl.commands to avoid circular dependency
import etl.config
import etl.config.env
import etl.config.log
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
import etl.monitor
import etl.names
import etl.pipeline
import etl.relation
import etl.s3
import etl.sync
import etl.templates
import etl.text
import etl.timer
import etl.unload
import etl.util
import etl.validate

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def load_tests(loader, tests, pattern):
    """
    Add tests within doctests so that the unittest runner picks them up.

    See https://docs.python.org/3.5/library/unittest.html#load-tests-protocol
    """
    etl_modules = sorted(mod for mod in sys.modules if mod.startswith("etl"))
    logger.info("Adding tests from %s", etl.names.join_with_single_quotes(etl_modules))
    for mod in etl_modules:
        tests.addTests(doctest.DocTestSuite(mod))
    return tests


def run_doctest(module_: Optional[str] = None, log_level: str = "INFO") -> None:
    verbosity_levels = {"DEBUG": 2, "INFO": 1, "WARNING": 0, "CRITICAL": 0}
    verbosity = verbosity_levels.get(log_level, 1)

    print("Running doctests...", flush=True)
    if module_ is None:
        module_ = __name__
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
        run_doctest()
    except Exception as exc:
        print(exc)
        sys.exit(1)
