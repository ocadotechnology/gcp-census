import webapp2

from gcp_census.bigquery.bigquery_handler import BigQueryMainHandler, \
    BigQueryTableHandler, BigQueryDatasetHandler, \
    BigQueryProjectHandler
from gcp_census.model.model_creator_handler import ModelCreatorHandler


class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.write('Welcome to gcp-census!')

app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/createModels', ModelCreatorHandler),
    ('/bigQuery', BigQueryMainHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>/dataset/<dataset_id:'
                  '.*>/table/<table_id:.*>', BigQueryTableHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>/dataset/'
                  '<dataset_id:.*>', BigQueryDatasetHandler),
    webapp2.Route('/bigQuery/project/<project_id:.*>',
                  BigQueryProjectHandler),
])