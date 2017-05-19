import unittest

from apiclient.http import HttpMockSequence

from google.appengine.ext import testbed
from mock import patch, Mock

from model.model_creator import ModelCreator
import test_utils


class TestModelCreator(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self.under_test = ModelCreator("bq_schemas")
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(ModelCreator, '_create_http')
    def test_should_create_dataset(self, _create_http):
        # given
        http_mock = Mock(wraps=HttpMockSequence([
            ({'status': '200'},
             test_utils.content(
                 'tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'}, ''),
        ]))
        _create_http.return_value = http_mock

        # when
        self.under_test.create_missing_datasets()

        # then
        calls = http_mock.mock_calls
        self.assertEqual(2, len(calls))
