import unittest

import etl.config.dw
import etl.errors


class TestConfigDW(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = {
            "owner": {"name": "etl", "group": "etl"},
            "users": [
                {
                    "name": "default",
                    "group": "some_group",
                },
                {
                    "name": "a_user",
                    "groups": ["a_group"],
                },
            ],
        }

    def test_parse_default_group(self):
        """Make sure we can load default group."""
        found = etl.config.dw.DataWarehouseConfig.parse_default_group(self.settings)
        self.assertEqual(found, "some_group")

    def test_parse_default_missing_group(self):
        """Make sure we error out if default group is missing."""
        settings = {"users": []}
        with self.assertRaises(etl.errors.ETLConfigError):
            etl.config.dw.DataWarehouseConfig.parse_default_group(settings)

    def test_parse_users(self):
        """Make sure we can load users."""
        found = etl.config.dw.DataWarehouseConfig.parse_users(self.settings)
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0].name, "etl")
        self.assertEqual(found[0].groups, ["etl"])
        self.assertEqual(found[1].name, "a_user")
        self.assertEqual(found[1].groups, ["a_group"])
