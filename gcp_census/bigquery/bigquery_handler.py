import logging

import webapp2
from googleapiclient.errors import HttpError

from gcp_census.bigquery.bigquery_client import BigQuery
from gcp_census.bigquery.bigquery_task import BigQueryTask


class BigQueryBaseClass(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(BigQueryBaseClass, self).__init__(request, response)
        self.bigquery = BigQuery()
        self.bigquery_task = BigQueryTask(self.bigquery)

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


class BigQueryMainHandler(BigQueryBaseClass):
    def get(self):
        self.bigquery_task.schedule_task_for_each_project()
        self.response.write("BigQuery process started. "
                            "Check the console for task progress.")


class BigQueryProjectHandler(BigQueryBaseClass):
    def get(self, project_id):
        self.bigquery_task.schedule_task_for_each_dataset(project_id)


class BigQueryDatasetHandler(BigQueryBaseClass):
    def get(self, project_id, dataset_id):
        page_token = self.request.get('pageToken', None)
        self.bigquery_task.schedule_task_for_each_table(project_id,
                                                        dataset_id,
                                                        page_token)


class BigQueryTableHandler(BigQueryBaseClass):
    def get(self, project_id, dataset_id, table_id):
        self.bigquery_task.stream_table_metadata(project_id, dataset_id,
                                                 table_id)
