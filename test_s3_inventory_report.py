from copy import copy
import unittest

from pyarrow import Table
import s3_inventory_report


class Aggregation(unittest.TestCase):
    """ Tests to ensure the aggregation returns expect results
    """

    def test_current_files(self):
        """
        Tests the aggregation of current files.
        """

        template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0, "Depth": 0}
        folders = {"/": copy(template)}
        sample = {
            "key": (
                "fa",
                "fb",
                "fc",
                "da/",
                "da/fa",
                "da/fb",
                "da/fc",
                "db/",
                "db/da/",
                "db/da/fa",
                "db/da/fb",
                "db/da/fc",
                ),
            "is_latest": (
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                ),
            "is_delete_marker": (
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                ),
            "size": (
                100,
                100,
                100,
                0,
                100,
                100,
                100,
                0,
                0,
                100,
                100,
                100,
                ),
        }

        table = Table.from_pydict(sample)
        s3_inventory_report.aggregate_folders(table, folders, None, template)

        expected = {
            '/': {'Count': 12, 'DelSize': 0, 'Depth': 0, 'Size': 900, 'VerSize': 0},
            'da/': {'Count': 4, 'DelSize': 0, 'Depth': 1, 'Size': 300, 'VerSize': 0},
            'db/': {'Count': 5, 'DelSize': 0, 'Depth': 1, 'Size': 300, 'VerSize': 0},
            'db/da/': {'Count': 4, 'DelSize': 0, 'Depth': 2, 'Size': 300, 'VerSize': 0}
        }

        self.assertEqual(folders, expected)


    def test_noncurrent_files(self):
        """
        Tests the aggregation of non current files.
        """

        template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0, "Depth": 0}
        folders = {"/": copy(template)}
        sample = {
            "key": ("fa", "fb", "fb", "fc",),
            "is_latest": (True, True, False, True,),
            "is_delete_marker": ( False, False, False, False,),
            "size": ( 100, 100, 50, 100,),
        }

        table = Table.from_pydict(sample)
        s3_inventory_report.aggregate_folders(table, folders, None, template)

        expected = {
            '/': {'Count': 4, 'DelSize': 0, 'Depth': 0, 'Size': 350, 'VerSize': 50},
        }

        self.assertEqual(folders, expected)


    def test_delete_marker_files(self):
        """
        Tests the aggregation of delete marker files.
        """

        template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0, "Depth": 0}
        folders = {"/": copy(template)}
        sample = {
            "key": ("fa", "fb", "fb", "fc",),
            "is_latest": (True, True, True, True,),
            "is_delete_marker": ( False, False, True, False,),
            "size": ( 100, 100, 50, 100,),
        }

        table = Table.from_pydict(sample)
        s3_inventory_report.aggregate_folders(table, folders, None, template)

        expected = {
            '/': {'Count': 4, 'DelSize': 50, 'Depth': 0, 'Size': 350, 'VerSize': 0},
        }

        self.assertEqual(folders, expected)


if __name__ == '__main__':
    unittest.main()
