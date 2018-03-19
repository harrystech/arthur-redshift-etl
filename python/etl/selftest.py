"""
"Self-test" of Arthur

We can run
* all the doctests from the source code
* static type checking against source code
"""

import doctest
import logging
import os.path
import sys
import unittest
from typing import Optional

import pep8


# Skip etl.commands to avoid circular dependency
import etl.config
import etl.design
import etl.design.bootstrap
import etl.design.load
import etl.data_warehouse
import etl.errors
import etl.explain
import etl.extract
import etl.file_sets
import etl.json_encoder
import etl.load
import etl.monitor
import etl.names
import etl.db
import etl.pipeline
import etl.relation
import etl.render_template
import etl.s3
import etl.sync
import etl.text
import etl.timer
import etl.unload
import etl.validate

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def run_pep8(module_: Optional[str]=None, log_level: str= "INFO") -> None:
    print("Running pep8...")
    if module_ is None:
        module_ = __name__
    quiet = log_level not in ("DEBUG", "INFO")
    style_guide = pep8.StyleGuide(parse_argv=False, quiet=quiet)
    report = style_guide.check_files(['python'])
    if report.total_errors > 0:
        raise etl.errors.SelfTestError("Unsuccessful (warning=%d, errors=%d)" %
                                       (report.get_count('W'), report.get_count('E')))
    print("OK")


def load_tests(loader, tests, pattern):
    """
    Add tests within doctests so that the unittest runner picks them up.

    See https://docs.python.org/3.5/library/unittest.html#load-tests-protocol
    """
    etl_modules = sorted(mod for mod in sys.modules if mod.startswith("etl"))
    logger.info("Adding tests from %s", etl.names.join_with_quotes(etl_modules))
    for mod in etl_modules:
        tests.addTests(doctest.DocTestSuite(mod))
    return tests


def run_doctest(module_: Optional[str]=None, log_level: str= "INFO") -> None:
    verbosity_levels = {"DEBUG": 2, "INFO": 1, "WARNING": 0, "CRITICAL": 0}
    verbosity = verbosity_levels.get(log_level, 1)

    print("Running doctests...")
    if module_ is None:
        module_ = __name__
    test_program = unittest.main(module=module_, verbosity=verbosity, exit=False, argv=sys.argv[:2])
    test_result = test_program.result
    if not test_result.wasSuccessful():
        raise etl.errors.SelfTestError("Unsuccessful (run=%d, errors=%d, failures=%d)" %
                                       (test_result.testsRun, len(test_result.errors), len(test_result.failures)))


def run_type_checker() -> None:
    print("Running type checker...")
    if not os.path.isdir("python"):
        raise etl.errors.ETLRuntimeError("Cannot find source directory: 'python'")

    # We wait with this import so that commands can be invoked in an environment where mypy is not installed.
    import mypy.api
    normal_report, error_report, exit_status = mypy.api.run(["python",  # Should match setup.py's package_dir
                                                             "--strict-optional",
                                                             "--ignore-missing-imports"])
    if normal_report:
        print("Type checking report:\n")
        print(normal_report)
    if error_report:
        print("Error report:\n")
        print(error_report)
    if exit_status != 0:
        raise etl.errors.SelfTestError("Unsuccessful (exit status = %d)" % exit_status)
    print("OK")


def run_tests() -> None:
    try:
        run_pep8()
        run_doctest()
        run_type_checker()
    except Exception as exc:
        print(exc)
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
