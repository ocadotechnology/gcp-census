import unittest

from gcp_census.bigquery.bigquery_table_metadata import BigQueryTableMetadata


class TestBigQueryTableMetadata(unittest.TestCase):

    def test_is_daily_partitioned_should_return_False_if_is_a_partition(self):
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

    def test_is_daily_partitioned_should_return_False_if_there_is_no_partitioning_field(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)

    def test_is_daily_partitioned_should_return_True_if_there_is_DAY_type_in_timePartitioning_field(
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

    def test_is_daily_partitioned_should_return_False_if_no_metadata_for_table(
            self
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata(None)
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)

    def test_is_partition_should_return_true_for_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.is_partition()
        # then
        self.assertEqual(True, result)

    def test_is_partition_should_return_false_for_table(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when
        result = big_query_table_metadata.is_partition()
        # then
        self.assertEqual(False, result)

    def test_get_partition_id_should_return_partition_id(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.get_partition_id()
        # then
        self.assertEqual("20171002", result)

    def test_get_partition_id_should_raise_exception_for_tables(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when then
        with self.assertRaises(AssertionError):
            big_query_table_metadata.get_partition_id()

    def test_get_table_id_should_return_table_id_when_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.get_table_id()
        # then
        self.assertEqual("t1", result)

    def test_get_table_id_should_return_table_id_when_not_partitioned(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when
        result = big_query_table_metadata.get_table_id()
        # then
        self.assertEqual("t1", result)
