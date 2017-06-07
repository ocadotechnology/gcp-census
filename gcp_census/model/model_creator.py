import os
import json
import logging

import httplib2
import googleapiclient.discovery
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from google.appengine.api import app_identity


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

    def list_models(self):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                for model_file in os.listdir(subdirectory):
                    model_name = os.path.splitext(model_file)[0]
                    with open(os.path.join(self.model_directory, group_dir,
                                           model_file)) as json_file:
                        json_dict = json.load(json_file)
                        yield Model(group_dir, model_name, json_dict)

    def list_groups(self):
        for group_dir in os.listdir(self.model_directory):
            subdirectory = os.path.join(self.model_directory, group_dir)
            if os.path.isdir(subdirectory):
                yield group_dir

    def create_missing_datasets(self):
        project_id = self.__get_project_id()
        location = os.getenv('BIGQUERY_LOCATION')
        for dataset_id in self.list_groups():
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

    def create_missing_models(self):
        project_id = self.__get_project_id()
        for model in self.list_models():
            logging.debug("Creating BQ table %s:%s.%s",
                          project_id, model.group, model.name)
            body = {
                'tableReference': {
                    'projectId': project_id,
                    'datasetId': model.group,
                    'tableId': model.name
                }
            }
            body.update(model.json_dict)
            try:
                self.service.tables().insert(
                    projectId=project_id, datasetId=model.group,
                    body=body
                ).execute()
                logging.info("Table %s:%s.%s created successfully", project_id,
                             model.group, model.name)
            except HttpError as e:
                if e.resp.status == 409:
                    logging.info("Table %s:%s.%s already exists", project_id,
                                 model.group, model.name)
                else:
                    raise e

    @staticmethod
    def __get_project_id():
        return os.getenv('BQ_STORAGE_PROJECT_ID',
                         app_identity.get_application_id())


class Model(object):
    def __init__(self, group, name, json_dict):
        self.group = group
        self.name = name
        self.json_dict = json_dict

    def __str__(self):
        return "Model(group={}, name={})".format(self.group, self.name)
