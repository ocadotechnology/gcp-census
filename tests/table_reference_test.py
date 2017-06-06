import unittest

from gcp_census.table_reference import TableReference


class TestTableReference(unittest.TestCase):

    def test_str(self):
        #given
        table = TableReference("project1", "dataset1", "table1")
        #when
        table_string = str(table)
        #then
        self.assertEqual(table_string, "project1:dataset1.table1")
