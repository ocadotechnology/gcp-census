import webapp2

from gcp_census.bigquery.gcp_metadata_handler import GcpStartMetadataHandler, \
    GcpTableMetadataHandler, GcpDatasetMetadataHandler, \
    GcpProjectMetadataHandler
from model.model_creator_handler import ModelCreatorHandler


class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.write('Welcome to gcp-census!')

app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/createModels', ModelCreatorHandler),
    ('/bigQuery', GcpStartMetadataHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>/dataset/<dataset_id:'
                  '.*>/table/<table_id:.*>', GcpTableMetadataHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>/dataset/'
                  '<dataset_id:.*>', GcpDatasetMetadataHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>',
                  GcpProjectMetadataHandler),
])