import logging

import googleapiclient.discovery
import httplib2
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

from gcp_census.config import Configuration


class ModelCreator(object):
    def __init__(self, model_provider):
        self.model_provider = model_provider
        self.http = self._create_http()
        self.service = googleapiclient.discovery.build(
            'bigquery',
            'v2',
            credentials=GoogleCredentials.get_application_default(),
            http=self.http
        )

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=10)

    def create_missing_datasets(self):
        project_id = Configuration.get_project_id()
        location = Configuration.get_default_location()
        for dataset_id in self.model_provider.list_groups():
            self.__create_dataset_if_missing(project_id, dataset_id, location)

    def __create_dataset_if_missing(self, project_id, dataset_id, location):
        logging.info("Creating dataset %s:%s in %s location",
                     project_id, dataset_id, location)
        body = {
            'datasetReference': {
                'projectId': project_id,
                'datasetId': dataset_id
            },
            'location': location
        }
        try:
            self.service.datasets().insert(
                projectId=project_id, body=body
            ).execute()
        except HttpError as e:
            if e.resp.status == 409:
                logging.info("Dataset %s:%s already exists", project_id,
                             dataset_id)
            else:
                raise e

    def create_missing_tables(self):
        project_id = Configuration.get_project_id()
        for table in self.model_provider.list_tables():
            logging.debug("Creating BQ table %s:%s.%s",
                          project_id, table.group, table.name)
            body = {
                'tableReference': {
                    'projectId': project_id,
                    'datasetId': table.group,
                    'tableId': table.name
                }
            }
            body.update(table.json_dict)
            try:
                self.service.tables().insert(
                    projectId=project_id, datasetId=table.group,
                    body=body
                ).execute()
                logging.info("Table %s:%s.%s created successfully", project_id,
                             table.group, table.name)
            except HttpError as e:
                if e.resp.status == 409:
                    logging.info("Table %s:%s.%s already exists", project_id,
                                 table.group, table.name)
                else:
                    raise e

    def create_missing_views(self):
        project_id = Configuration.get_project_id()
        for view in self.model_provider.list_views():
            logging.debug("Creating BQ view %s:%s.%s",
                          project_id, view.group, view.name)
            body = {
                'tableReference': {
                    'projectId': project_id,
                    'datasetId': view.group,
                    'tableId': view.name
                },
                "view": {
                    "query": view.query
                },
                "description": view.description
            }
            try:
                self.service.tables().insert(
                    projectId=project_id, datasetId=view.group,
                    body=body
                ).execute()
                logging.info("View %s:%s.%s created successfully", project_id,
                             view.group, view.name)
            except HttpError as e:
                if e.resp.status == 409:
                    logging.info("View %s:%s.%s already exists", project_id,
                                 view.group, view.name)
                else:
                    raise e
