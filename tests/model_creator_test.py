import unittest

from apiclient.errors import HttpError
from apiclient.http import HttpMockSequence
from google.appengine.ext import testbed
from mock import patch, Mock

import test_utils
from gcp_census.model.model_creator import ModelCreator
from gcp_census.model.table import Table
from gcp_census.model.view import View


class TestModelCreator(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(ModelCreator, '_create_http')
    def test_should_create_dataset(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_datasets_insert_200.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_datasets_insert_200.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_groups.return_value = ["missing_dataset1",
                                                   "missing_dataset2"]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_datasets()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))

    @patch.object(ModelCreator, '_create_http')
    def test_should_ignore_dataset_already_exists_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_datasets_insert_409.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_datasets_insert_409.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_groups.return_value = ["existing_dataset1",
                                                   "existing_dataset2"]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_datasets()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))

    @patch.object(ModelCreator, '_create_http')
    def test_should_propagate_dataset_500_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '500'}, '')
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_groups.return_value = ["missing_dataset1"]

        under_test = ModelCreator(model_provider)

        # when
        with self.assertRaises(HttpError) as context:
            under_test.create_missing_datasets()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(2, len(calls))
        self.assertEqual(500, context.exception.resp.status)

    @patch.object(ModelCreator, '_create_http')
    def test_should_create_tables(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_200.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_200.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_tables.return_value = [Table("group", "name1", {}),
                                                   Table("group", "name2", {})]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_tables()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))

    @patch.object(ModelCreator, '_create_http')
    def test_should_ignore_table_already_exists_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_409.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_409.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_tables.return_value = [Table("group", "name1", {}),
                                                   Table("group", "name2", {})]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_tables()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))

    @patch.object(ModelCreator, '_create_http')
    def test_should_propagate_table_500_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '500'}, '')
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_tables.return_value = [Table("group", "name1", {})]

        under_test = ModelCreator(model_provider)

        # when
        with self.assertRaises(HttpError) as context:
            under_test.create_missing_tables()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(2, len(calls))
        self.assertEqual(500, context.exception.resp.status)

    @patch.object(ModelCreator, '_create_http')
    def test_should_create_views(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_200.json')),
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_200.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_views. \
            return_value = [View("group", "name1", "SELECT * FROM TABLE"),
                            View("group", "name2", "SELECT * FROM TABLE")]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_views()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))
        json_request = test_utils.get_body_from_http_request(
            calls[1])
        self.assertTrue('description' in json_request)
        self.assertTrue('query' in json_request['view'])

    @patch.object(ModelCreator, '_create_http')
    def test_should_ignore_view_already_exists_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_409.json')),
            ({'status': '409'}, test_utils.content(
                'tests/json_samples/bigquery_v2_tables_insert_409.json'))
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_views. \
            return_value = [View("group", "name1", "SELECT * FROM TABLE"),
                            View("group", "name2", "SELECT * FROM TABLE")]

        under_test = ModelCreator(model_provider)

        # when
        under_test.create_missing_views()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(3, len(calls))

    @patch.object(ModelCreator, '_create_http')
    def test_should_propagate_view_500_error(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'}, test_utils.content(
                'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '500'}, '')
        ]))
        _create_http.return_value = http_mock
        model_provider = Mock()
        model_provider.list_views. \
            return_value = [View("group", "name1", "SELECT * FROM TABLE")]

        under_test = ModelCreator(model_provider)

        # when
        with self.assertRaises(HttpError) as context:
            under_test.create_missing_views()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(2, len(calls))
        self.assertEqual(500, context.exception.resp.status)
