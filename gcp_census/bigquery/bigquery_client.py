import json
import logging
import datetime
from google.appengine.api.app_identity import app_identity

from gcp_census.decorators import retry, log_time, measure_time_and_log
import googleapiclient.discovery

import httplib2  # nopep8 pylint: disable=C0413
from apiclient.errors import HttpError, Error  # nopep8 pylint: disable=C0413
from oauth2client.client import GoogleCredentials  # nopep8 pylint: disable=C0413


class BigQuery(object): # pylint: disable=R0904
    def __init__(self):
        self.http = self._create_http()
        self.service = googleapiclient.discovery.build(
            'bigquery',
            'v2',
            credentials=GoogleCredentials.get_application_default(),
            http=self.http
        )

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=60)

    def list_project_ids(self):
        request = self.service.projects().list()
        while request is not None:
            with measure_time_and_log('Request for projects list'):
                projects = request.execute()

            for project in projects['projects']:
                yield project['projectReference']['projectId']

            request = self.service.projects().list_next(request, projects)

    def list_dataset_ids(self, project_id):
        request = self.service.datasets().list(projectId=project_id)
        while request is not None:
            with measure_time_and_log('Request for dataset table list'):
                datasets = request.execute(num_retries=5)

            if 'datasets' in datasets:
                for dataset in datasets['datasets']:
                    yield dataset['datasetReference']['datasetId']
            request = self.service.datasets().list_next(request, datasets)

    def list_table_ids(self, project_id, dataset_id):
        request = self.service.tables().list(
            projectId=project_id, datasetId=dataset_id
        )
        while request is not None:
            tables = request.execute()
            if 'tables' in tables:
                for table in tables['tables']:
                    if 'tableReference' not in table:
                        logging.info('tableReference not in table,  '
                                     'table= "%s"',
                                     json.dumps(table))
                    if 'tableId' not in table['tableReference']:
                        logging.info('tableId not in table reference, '
                                     'tableReference = "%s"',
                                     json.dumps(table["tableReference"]))
                    yield table['tableReference']['tableId']
            request = self.service.tables().list_next(request, tables)

    @retry(Error, tries=3, delay=2, backoff=2)
    def list_tables(self, project_id, dataset_id, page_token=None,
                    max_results=1000):
        return self.service.tables().list(
            projectId=project_id, datasetId=dataset_id,
            maxResults=max_results, pageToken=page_token
        ).execute()

    @log_time
    def list_table_partitions(self, project_id, dataset_id, table_id):
        query = self.create_partition_query(project_id, dataset_id,
                                            table_id)
        query_job = self.sync_query(query=query, use_legacy_sql=True)

        results = []
        page_token = None

        while True:
            page = self.service.jobs().getQueryResults(
                pageToken=page_token,
                **query_job['jobReference']).execute(num_retries=2)

            results.extend(page.get('rows', []))

            page_token = page.get('pageToken')
            if not page_token:
                break
        partitions = [
            {'partitionId': _partition['f'][0]['v'],
             'creationTime': _partition['f'][1]['v'],
             'lastModifiedTime': _partition['f'][2]['v']}
            for _partition in results
        ]
        return partitions

    @staticmethod
    def create_partition_query(project_id, dataset_id, table_id):
        return "SELECT partition_id, FORMAT_UTC_USEC(creation_time*1000) AS " \
               "creation_time, FORMAT_UTC_USEC(last_modified_time*1000)" \
               " AS last_modified FROM [{0}:{1}.{2}$__PARTITIONS_SUMMARY__]"\
            .format(project_id, dataset_id, table_id)

    def sync_query(self, query, timeout=30000, use_legacy_sql=False):
        query_data = {
            'query': query,
            'timeoutMs': timeout,
            'useLegacySql': use_legacy_sql
        }
        return self.service.jobs().query(
            projectId=app_identity.get_application_id(),
            body=query_data).execute(num_retries=3)

    @retry(HttpError, tries=6, delay=2, backoff=2)
    def get_table(self, project_id, dataset_id, table_id):
        try:
            table = self.service.tables().get(
                projectId=project_id, datasetId=dataset_id, tableId=table_id
            ).execute(num_retries=3)

            return table
        except HttpError as error:
            logging.info('Can\'t fetch table: %s', error.resp)
            if error.resp.status == 404:
                logging.warning(
                    'Table not found (404) but known to be missing and '
                    'filtered out (%s, %s, %s): \n\n%s").',
                    project_id, dataset_id, table_id, error.resp
                )
            else:
                raise HttpError(error.resp, error.content)

    def stream_stats(self, row):
        insert_all_data = {
            'rows': [{
                'json': row.data,
                'insertId': row.insert_id
            }]
        }
        insert_all_response = self._stream_metadata(insert_all_data,
                                                    row.dataset_id,
                                                    row.table_id)
        if 'insertErrors' in insert_all_response:
            logging.debug("Sent json: \n%s", json.dumps(insert_all_data))
            logging.error("Error during streaming metadata to BigQuery: \n%s",
                          json.dumps(insert_all_response['insertErrors']))

    @retry(Error, tries=2, delay=2, backoff=2)
    def _stream_metadata(self, insert_all_data, dataset_id, table_id):
        partition = datetime.datetime.now().strftime("%Y%m%d")
        return self.service.tabledata().insertAll(
            projectId=app_identity.get_application_id(),
            datasetId=dataset_id,
            tableId='{}${}'.format(table_id, partition),
            body=insert_all_data).execute(num_retries=3)
