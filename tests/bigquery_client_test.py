import unittest

import webtest
from apiclient.http import HttpMockSequence

from gcp_census import routes
from gcp_census.bigquery.bigquery_client import BigQuery
from google.appengine.ext import testbed
from mock import patch

import test_utils
from gcp_census.bigquery.row import Row


class TestBigQuery(unittest.TestCase):
    def setUp(self):
        self.under_test = webtest.TestApp(routes.app)
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
             test_utils.content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_project_list_page_1.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_project_list_page_last.json'))
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
             test_utils.content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_dataset_list_page_1.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_dataset_list_page_last.json'))
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
             test_utils.content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_table_list_page_1.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_table_list_page_last.json'))
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
             test_utils.content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_query_for_partitions.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/'
                     'bigquery_v2_query_for_partitions_results_1.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/'
                     'bigquery_v2_query_for_partitions_results_last.json'))
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
    def test_streaming_row(self, _stream_metadata, _create_http):  # nopep8 pylint: disable=W0613
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             test_utils.content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             test_utils.content('tests/json_samples/'
                     'bigquery_v2_stream_response.json'))
        ])
        under_test = BigQuery()
        data = {'key': 'value'}
        row = Row('dataset_id', 'table_id', 'insert_id', data=data)

        # when
        under_test.stream_stats(row)

        # then
        stream_data = _stream_metadata.call_args_list[0][0][0]
        json_payload = stream_data['rows'][0]['json']
        insert_id = stream_data['rows'][0]['insertId']
        self.assertEqual(insert_id, row.insert_id)
        self.assertEqual(json_payload, row.data)

    @staticmethod
    def count(generator):
        return sum(1 for _ in generator)
