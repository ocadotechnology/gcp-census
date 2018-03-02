import webapp2

from gcp_census.model.filesystem_model_provider import FilesystemModelProvider
from gcp_census.model.model_creator import ModelCreator


class ModelCreatorHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(ModelCreatorHandler, self).__init__(request, response)

    def get(self):
        model_creator = ModelCreator(FilesystemModelProvider("bq_schemas"))
        model_creator.create_missing_datasets()
        model_creator.create_missing_tables()
        model_creator.create_missing_views()
