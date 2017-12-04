import os
from google.appengine.api import app_identity


class Configuration(object):

    @staticmethod
    def get_project_id():
        return os.getenv('BQ_STORAGE_PROJECT_ID',
                         app_identity.get_application_id())

    @staticmethod
    def get_default_location():
        return os.getenv('BIGQUERY_LOCATION')
