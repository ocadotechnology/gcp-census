import unittest
import os.path
from mock import patch
import webtest
from google.appengine.ext import testbed
from apiclient.http import HttpMockSequence

import main
from gcp_census.big_query_table_metadata import BigQueryTableMetadata
from gcp_census.bigquery import gcp_metadata_handler
from gcp_census.bigquery_client import BigQuery


def content(filename):
    if not os.path.exists(filename):
        raise Exception("File not found: {0}".format(filename))
    with open(filename, 'r') as f:
        return f.read()


class TestBigQuery(unittest.TestCase):
    def setUp(self):
        self.under_test = webtest.TestApp(main.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(BigQuery, '_create_http')
    def test_iterating_projects(self,
                                _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_project_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_project_list_page_last.json'))
        ])

        under_test = BigQuery()

        # when
        project_ids = under_test.list_project_ids()

        # then
        self.assertEqual(self.count(project_ids), 4)

    @patch.object(BigQuery, '_create_http')
    def test_iterating_datasets(self,
                                _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_list_page_last.json'))
        ])

        under_test = BigQuery()

        # when
        dataset_ids = under_test.list_dataset_ids("project123")

        # then
        self.assertEqual(self.count(dataset_ids), 3)

    @patch.object(BigQuery, '_create_http')
    def test_iterating_tables(self,
                              _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_last.json'))
        ])

        under_test = BigQuery()

        # when
        dataset_ids = under_test.list_table_ids("project123", "dataset_id")

        # then
        self.assertEqual(self.count(dataset_ids), 5)



    @patch.object(BigQuery, '_create_http')
    def test_listing_table_partitions(self,
                                      _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_query_for_partitions.json')),
            ({'status': '200'},
             content('tests/json_samples/'
                     'bigquery_query_for_partitions_results_1.json')),
            ({'status': '200'},
             content('tests/json_samples/'
                     'bigquery_query_for_partitions_results_last.json'))
        ])
        under_test = BigQuery()

        # when
        partitions = under_test.list_table_partitions("project123",
                                                      "dataset123", "table123")

        # then
        self.assertEqual(self.count(partitions), 5)
        self.assertEqual(partitions[0]['partitionId'], '20170317')
        self.assertEqual(partitions[0]['creationTime'],
                         '2017-03-17 14:32:17.755000')
        self.assertEqual(partitions[0]['lastModifiedTime'],
                         '2017-03-17 14:32:19.289000')

    @patch.object(BigQuery, '_create_http')
    @patch.object(BigQuery, '_stream_metadata')
    def test_streaming_table_stats(self,
                                   _stream_metadata, _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_stream_response.json')),
        ])
        under_test = BigQuery()
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'sit-atm-eu-datamanager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'sit-atm-eu-datamanager',
                'datasetId': 'crm_raw',
                'tableId': 'account_1_0_0_20150603'
            },
            "description": "secs\n\njhbhgvhgv\n\nlorem",
            'numBytes': '421940',
            'numLongTermBytes': '421940',
            'numRows': '1445',
            'creationTime': '1468414660572',
            'lastModifiedTime': '1468414660572',
            'type': 'TABLE',
            'location': 'US'
        }

        # when
        under_test.stream_stats(BigQueryTableMetadata(table), partitions=[])

        # then
        stream_data = _stream_metadata.call_args_list[0][0][0]
        json_payload = stream_data['rows'][0]['json']
        self.assertEqual('account_1_0_0_20150603', json_payload['tableId'])
        self.assertEqual(0, len(json_payload['labels']))

    @patch.object(BigQuery, '_create_http')
    @patch.object(BigQuery, '_stream_metadata')
    def test_streaming_table_stats_with_labels(self,
                                               _stream_metadata, _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_stream_response.json')),
        ])
        under_test = BigQuery()
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'sit-atm-eu-datamanager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'sit-atm-eu-datamanager',
                'datasetId': 'crm_raw',
                'tableId': 'account_1_0_0_20150603'
            },
            "description": "secs\n\njhbhgvhgv\n\nlorem",
            "labels": {
                "key1": "value1",
                "empty": "",
                "key3": "value3"
            },
            'numBytes': '421940',
            'numLongTermBytes': '421940',
            'numRows': '1445',
            'creationTime': '1468414660572',
            'lastModifiedTime': '1468414660572',
            'type': 'TABLE',
            'location': 'US'
        }

        # when
        under_test.stream_stats(BigQueryTableMetadata(table), partitions=[])

        # then
        stream_data = _stream_metadata.call_args_list[0][0][0]
        json_payload = stream_data['rows'][0]['json']
        self.assertEqual(3, len(json_payload['labels']))
        self.assertEqual(json_payload['labels'][0]['key'], 'key3')
        self.assertEqual(json_payload['labels'][0]['value'], 'value3')

    @staticmethod
    def count(generator):
        return sum(1 for _ in generator)
