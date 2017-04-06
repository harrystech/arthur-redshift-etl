"""
"Self-test" of Arthur
"""

import doctest
import logging
import unittest
import sys

from etl.errors import SelfTestError
from etl.names import join_with_quotes


def load_tests(loader, tests, pattern):
    # https://docs.python.org/3.5/library/unittest.html#load-tests-protocol
    logger = logging.getLogger(__name__)
    etl_modules = sorted(mod for mod in sys.modules if mod.startswith("etl"))
    logger.info("Adding tests from %s", join_with_quotes(etl_modules))
    for mod in etl_modules:
        tests.addTests(doctest.DocTestSuite(mod))
    return tests


def run_self_test(verbosity):
    # TODO turn logging off while running tests
    test_program = unittest.main(module="etl", verbosity=verbosity, exit=False)
    test_result = test_program.result
    if not test_result.wasSuccessful():
        raise SelfTestError("Unsuccessful (run=%d, errors=%d, failures=%d)" %
                            (test_result.testsRun, len(test_result.errors), len(test_result.failures)))


if __name__ == "__main__":
    unittest.main(argv=sys.argv)
