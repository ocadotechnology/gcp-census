import unittest

import webapp2

import webtest
from google.appengine.ext import testbed
from mock import patch

from gcp_census.model.model_creator import ModelCreator
from gcp_census.model.model_creator_handler import ModelCreatorHandler


class TestModelCreatorHandler(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        app = webapp2.WSGIApplication(
            [('/createModels', ModelCreatorHandler)]
        )
        self.under_test = webtest.TestApp(app)

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(ModelCreator, 'create_missing_datasets')
    @patch.object(ModelCreator, 'create_missing_tables')
    @patch.object(ModelCreator, 'create_missing_views')
    def test_happy_path(self, create_missing_views, create_missing_tables,
                        create_missing_datasets):
        # when
        response = self.under_test.get(
            '/createModels'
        )
        # then
        self.assertEqual(response.status_int, 200)
        create_missing_views.assert_called_once()
        create_missing_tables.assert_called_once()
        create_missing_datasets.assert_called_once()
