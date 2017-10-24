import logging
import unittest

import os
import webtest
from google.appengine.ext import testbed
from googleapiclient.errors import HttpError
from httplib2 import Response
from mock import patch

from gcp_census import routes
from gcp_census.bigquery.bigquery_client import BigQuery
from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1

response404 = Response({"status": 404, "reason": "Table Not found"})
response500 = Response({"status": 500, "reason": "Internal error"})

example_table = {
    'tableReference': {
        'projectId': 'myproject123',
        'datasetId': 'd1',
        'tableId': 't1',
    }
}


class TestGcpMetadataHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(routes.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

        path = os.path.join(os.path.dirname(__file__), '../config')
        logging.debug("queue.yaml path: %s", path)
        self.testbed.init_taskqueue_stub(root_path=path)
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)
        self.testbed.init_app_identity_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(BigQuery, 'list_project_ids', return_value=["p1", "p2", "p3"])
    def test_that_start_will_schedule_project(
            self, list_project_ids
    ):
        # given

        # when
        self.under_test.get('/bigQuery')

        # then
        list_project_ids.assert_called_once()
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 3)
        self.assertEqual(tasks[0].url, '/bigQuery/project/p1')
        self.assertEqual(tasks[1].url, '/bigQuery/project/p2')
        self.assertEqual(tasks[2].url, '/bigQuery/project/p3')

    @patch.object(BigQuery, 'list_dataset_ids', return_value=["d1", "d2"])
    def test_that_project_endpoint_will_schedule_dataset_tasks(
            self, list_dataset_ids
    ):
        # given

        # when
        self.under_test.get('/bigQuery/project/myproject123',
                            expect_errors=False)

        # then
        list_dataset_ids.assert_called_once_with('myproject123')
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url,
                         '/bigQuery/project/myproject123/dataset/d1')
        self.assertEqual(tasks[1].url,
                         '/bigQuery/project/myproject123/dataset/d2')

    @patch.object(BigQuery, 'list_tables', return_value={})
    def test_that_dataset_endpoint_will_ignore_empty_dataset(
            self, list_tables
    ):
        # given

        # when
        self.under_test.get('/bigQuery/project/myproject123/dataset/d1')

        # then
        list_tables.assert_called_once_with('myproject123', 'd1',
                                            page_token=None)
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 0)

    @patch.object(BigQuery, 'list_tables')
    def test_that_dataset_endpoint_will_schedule_initial_table_tasks(
            self, list_tables
    ):
        # given
        list_tables.return_value = {
            'nextPageToken': 'abc123',
            'tables': [
                {
                    'tableReference': {
                        'tableId': 't1'
                    }
                },
                {
                    'tableReference': {
                        'tableId': 't2'
                    }
                },
                {
                    'tableReference': {
                        'tableId': 't3'
                    }
                }
            ]
        }

        # when
        self.under_test.get('/bigQuery/project/myproject123/dataset/d1')

        # then
        list_tables.assert_called_once_with('myproject123', 'd1',
                                            page_token=None)
        table_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='bigquery-tables')
        self.assertEqual(len(table_tasks), 3)
        self.assertEqual(table_tasks[0].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t1')
        self.assertEqual(table_tasks[1].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t2')
        self.assertEqual(table_tasks[2].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t3')
        list_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='bigquery-list')
        self.assertEqual(len(list_tasks), 1)
        self.assertEqual(list_tasks[0].url,
                         '/bigQuery/project/myproject123/dataset/d1'
                         '?pageToken=abc123')

    @patch.object(BigQuery, 'list_tables')
    def test_that_dataset_endpoint_will_schedule_next_table_tasks(
            self, list_tables
    ):
        # given
        list_tables.return_value = {
            'tables': [
                {
                    'tableReference': {
                        'tableId': 't4'
                    }
                },
                {
                    'tableReference': {
                        'tableId': 't5'
                    }
                },
                {
                    'tableReference': {
                        'tableId': 't6'
                    }
                }
            ]
        }

        # when
        self.under_test.get('/bigQuery/project/myproject123/dataset/d1'
                            '?pageToken=abc123')

        # then
        list_tables.assert_called_once_with('myproject123', 'd1',
                                            page_token='abc123')
        table_tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(table_tasks), 3)
        self.assertEqual(table_tasks[0].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t4')
        self.assertEqual(table_tasks[1].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t5')
        self.assertEqual(table_tasks[2].url,
                         '/bigQuery/project/myproject123/'
                         'dataset/d1/table/t6')

    @patch.object(BigQuery, 'list_tables',
                  side_effect=[HttpError(response404, ''), None])
    def test_that_dataset_endpoint_will_ignore_404_error(
            self, list_tables
    ):
        # given

        # when
        self.under_test.get('/bigQuery/project/myproject123/dataset/d1')

        # then
        list_tables.assert_called_once_with('myproject123', 'd1',
                                            page_token=None)
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 0)

    @patch.object(BigQuery, 'list_tables',
                  side_effect=[HttpError(response500, ''), None])
    def test_that_dataset_endpoint_will_throw_500_error(
            self, list_tables
    ):
        # given

        # when
        response = self.under_test.get('/bigQuery/project/myproject123/'
                                       'dataset/d1', expect_errors=True)

        # then
        self.assertEqual(response.status_code, 500)
        list_tables.assert_called_once_with('myproject123', 'd1',
                                            page_token=None)
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 0)

    @patch.object(BigQuery, 'get_table', return_value=example_table)
    @patch.object(BigQuery, 'stream_stats')
    @patch.object(TableMetadataV0_1, 'transform')
    def test_streaming_table_metadata(
            self, _, stream_stats, get_table
    ):
        # given

        # when
        self.under_test.get('/bigQuery/project/myproject123/dataset/d1/'
                            'table/t1')

        # then
        get_table.assert_called_once_with('myproject123', 'd1', 't1',
                                          log_table=False)
        stream_stats.assert_called_once()
