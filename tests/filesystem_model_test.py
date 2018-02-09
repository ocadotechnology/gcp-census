import unittest

from gcp_census.model.filesystem_model_provider import FilesystemModelProvider


class TestFilesystemModelProvider(unittest.TestCase):

    def test_should_return_at_least_3_groups(self):
        # given
        under_test = FilesystemModelProvider("bq_schemas")

        # when
        groups = list(under_test.list_groups())

        # then
        self.assertGreaterEqual(len(groups), 3)
        self.assertTrue("bigquery" in groups)
        self.assertTrue("bigquery_views" in groups)
        self.assertTrue("bigquery_views_legacy_sql" in groups)

    def test_should_return_at_least_3_tables(self):
        # given
        under_test = FilesystemModelProvider("bq_schemas")

        # when
        tables = list(under_test.list_tables())

        # then
        self.assertGreaterEqual(len(tables), 3)
        table_names = [t.name for t in tables]
        self.assertTrue("partition_metadata_v1_0" in table_names)
        self.assertTrue("table_metadata_v1_0" in table_names)

    def test_should_return_at_least_4_views(self):
        # given
        under_test = FilesystemModelProvider("bq_schemas")

        # when
        views = list(under_test.list_views())

        # then
        self.assertGreaterEqual(len(views), 4)
        view_names = [v.name for v in views]
        self.assertTrue("partition_metadata_v1_0" in view_names)
        self.assertTrue("table_metadata_v1_0" in view_names)
