import unittest
from mock import patch

from gcp_census.big_query_table_metadata import BigQueryTableMetadata


class TestBigQueryTableMetadata(unittest.TestCase):
    def setUp(self):
        patch(
            'Environment.Environment.version_id',
            return_value='dummy_version'
        ).start()
        patch(
            'Configuration.Configuration.project_id',
            return_value='dummy_version'
        ).start()

    def tearDown(self):
        patch.stopall()

    def test_should_return_False_if_is_a_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata(
            {"tableReference":
                 {"tableId": "tableName$20170324"},
             "timePartitioning": {"type": "DAY"}
             }
        )
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)

    def test_should_return_False_if_there_is_no_partitioning_field(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)

    def test_should_return_True_if_there_is_DAY_type_in_timePartitioning_field(
            self
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {"tableId": "tableName"},
            "timePartitioning": {"type": "DAY"}
        })
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(True, result)

    def test_should_return_False_if_no_metadata_for_table(
            self
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata(None)
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)
