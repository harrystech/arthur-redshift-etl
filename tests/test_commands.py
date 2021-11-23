import unittest
import unittest.mock

import etl.commands


class TestCommands(unittest.TestCase):
    def test_execute_or_bail_ok(self):
        """Make sure context runner exits cleanly."""
        with self.assertLogs(level="INFO") as cm:
            with etl.commands.execute_or_bail("unittest"):
                pass
        self.assertEqual(len(cm.output), 1)
        self.assertTrue("finished successfully" in cm.output[0])

    def test_execute_or_bail_internal_error(self):
        """Make sure context runner exits with error message for internal error."""
        with self.assertLogs(level="INFO") as cm:
            # This patches the "croak" function that is IMPORTED inside "etl.commands".
            with unittest.mock.patch("etl.commands.croak") as mocked_croak:
                with etl.commands.execute_or_bail("unittest"):
                    # Simulate an internal error where we don't catch an exception.
                    raise ValueError("oops")
        mocked_croak.assert_called()
        # The exit code (2nd arg) is expected to be 3 for uncaught exceptions.
        self.assertEqual(mocked_croak.call_args[0][1], 3)

        self.assertEqual(len(cm.output), 2)
        self.assertIn("terrible happened", cm.output[0])
