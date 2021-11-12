import unittest

import etl.util.timer


class TestTimer(unittest.TestCase):
    def test_aware(self):
        """Make sure utc_now has a timezone."""
        now = etl.util.timer.utc_now()
        self.assertIsNotNone(now.tzinfo)

    def test_no_elapsed_time(self):
        """Make sure difference is 0 for same timestamps."""
        now = etl.util.timer.utc_now()
        self.assertEqual(etl.util.timer.elapsed_seconds(now, now), 0.0)

    def test_context_manager(self):
        """Make sure we can retrieve elapsed seconds."""
        with etl.util.timer.Timer() as my_timer:
            self.assertEqual(str(my_timer), "0.00s")
