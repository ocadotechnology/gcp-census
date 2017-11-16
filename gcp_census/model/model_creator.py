import os
import json
import logging

import httplib2
import googleapiclient.discovery
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from google.appengine.api import app_identity

from gcp_census.model.table import Table
from gcp_census.model.view import View


class ModelCreator(object):
    def __init__(self, model_directory):
        self.model_directory = model_directory
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

    def __list_tables(self):
        for table in self.__list_files('.json'):
            with open(table[2]) as json_file:
                json_dict = json.load(json_file)
                yield Table(table[0], table[1], json_dict)

    def __list_views(self):
        for view in self.__list_files('.sql'):
            with open(view[2]) as view_file:
                content = view_file.readlines()
                yield View(view[0], view[1], content)

    def __list_files(self, extension):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                for model_file in os.listdir(subdirectory):
                    if model_file.endswith(extension):
                        model_name = os.path.splitext(model_file)[0]
                        filename = os.path.join(self.model_directory, group_dir,
                                            model_file)
                        yield group_dir, model_name, filename

    def __list_groups(self):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                yield group_dir

    def create_missing_datasets(self):
        project_id = self.__get_project_id()
        location = os.getenv('BIGQUERY_LOCATION')
        for dataset_id in self.__list_groups():
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
        project_id = self.__get_project_id()
        for table in self.__list_tables():
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
        project_id = self.__get_project_id()
        for view in self.__list_views():
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

    @staticmethod
    def __get_project_id():
        return os.getenv('BQ_STORAGE_PROJECT_ID',
                         app_identity.get_application_id())


