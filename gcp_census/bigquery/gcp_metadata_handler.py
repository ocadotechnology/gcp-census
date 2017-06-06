import logging
import webapp2
from googleapiclient.errors import HttpError

from gcp_census.bigquery.gcp_metadata_task import GcpMetadataTask
from gcp_census.bigquery_client import BigQuery


class GcpMetadataBaseClass(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(GcpMetadataBaseClass, self).__init__(request, response)
        self.big_query = BigQuery()
        self.gcp_metadata_task = GcpMetadataTask(self.big_query)

    def handle_exception(self, exception, debug): # nopep8 pylint: disable=W0613
        logging.exception(exception)
        if isinstance(exception, HttpError):
            if exception.resp.status == 404:
                logging.info("Received 404 error code, task won't be retried")
                self.response.set_status(200)
            else:
                self.response.set_status(exception.resp.status)
        else:
            self.response.set_status(500)


class GcpStartMetadataHandler(GcpMetadataBaseClass):
    def get(self):
        self.gcp_metadata_task.schedule_task_for_each_project()
        self.response.write("GCP metadata process started. "
                            "Check the console for task progress.")


class GcpProjectMetadataHandler(GcpMetadataBaseClass):
    def get(self, project_id):
        self.gcp_metadata_task.schedule_task_for_each_dataset(project_id)


class GcpDatasetMetadataHandler(GcpMetadataBaseClass):
    def get(self, project_id, dataset_id):
        page_token = self.request.get('pageToken', None)
        self.gcp_metadata_task.schedule_task_for_each_table(project_id,
                                                            dataset_id,
                                                            page_token)


class GcpTableMetadataHandler(GcpMetadataBaseClass):
    def get(self, project_id, dataset_id, table_id):
        self.gcp_metadata_task.stream_table_metadata(project_id, dataset_id,
                                                     table_id)


app = webapp2.WSGIApplication([
    ('/gcp_metadata/start', GcpStartMetadataHandler),
    webapp2.Route('/gcp_metadata/project/<project_id:.*>/dataset/<dataset_id:'
                  '.*>/table/<table_id:.*>', GcpTableMetadataHandler),
    webapp2.Route('/gcp_metadata/project/<project_id:.*>/dataset/'
                  '<dataset_id:.*>', GcpDatasetMetadataHandler),
    webapp2.Route('/gcp_metadata/project/<project_id:.*>',
                  GcpProjectMetadataHandler),
])
