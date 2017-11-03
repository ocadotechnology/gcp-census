import unittest
from mock import patch

from gcp_census.bigquery.bigquery_client import BigQuery
from gcp_census.bigquery.bigquery_table_metadata import BigQueryTableMetadata
from gcp_census.bigquery.bigquery_table_streamer import BigQueryTableStreamer
from gcp_census.bigquery.transformers.partition_metadata_v1_0 import \
    PartitionMetadataV1_0
from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1
from gcp_census.bigquery.transformers.table_metadata_v1_0 import \
    TableMetadataV1_0


class TestBigQueryTableStreamer(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(TableMetadataV0_1, 'transform')
    @patch.object(TableMetadataV1_0, 'transform')
    @patch.object(PartitionMetadataV1_0, 'transform')
    @patch.object(BigQuery, '_stream_metadata')
    def test_should_stream_partition_table_to_v1_tables_only(self,
                                                             _,
                                                             partition_v1_0,
                                                             table_v1_0,
                                                             table_v0_1):
        # given
        table_streamer = BigQueryTableStreamer(BigQuery())
        table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })

        # when
        table_streamer.stream_metadata(table_metadata, [])
        # then
        table_v0_1.assert_not_called()
        table_v1_0.assert_not_called()
        partition_v1_0.assert_called_once()

    @patch.object(TableMetadataV0_1, 'transform')
    @patch.object(TableMetadataV1_0, 'transform')
    @patch.object(PartitionMetadataV1_0, 'transform')
    @patch.object(BigQuery, '_stream_metadata')
    def test_should_stream_table_to_v1_tables_only(self,
                                                             _,
                                                             partition_v1_0,
                                                             table_v1_0,
                                                             table_v0_1):
        # given
        table_streamer = BigQueryTableStreamer(BigQuery())
        table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })

        # when
        table_streamer.stream_metadata(table_metadata, [])
        # then
        table_v0_1.assert_called_once()
        table_v1_0.assert_called_once()
        partition_v1_0.assert_not_called()
