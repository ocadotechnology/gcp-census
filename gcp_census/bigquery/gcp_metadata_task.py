import logging
from google.appengine.api.taskqueue import Task

from gcp_census.big_query_table_metadata import BigQueryTableMetadata
from gcp_census.tasks import Tasks


class GcpMetadataTask(object):
    def __init__(self, big_query):
        self.big_query = big_query

    def schedule_task_for_each_project(self):
        tasks = self.create_project_tasks(self.big_query.list_project_ids())
        Tasks.schedule(queue_name='gcp-metadata', tasks=tasks)

    def schedule_task_for_each_dataset(self, project_id):
        tasks = self.create_dataset_tasks(project_id, self.big_query.
                                          list_dataset_ids(project_id))
        Tasks.schedule(queue_name='gcp-metadata', tasks=tasks)

    def schedule_task_for_each_table(self, project_id, dataset_id,
                                     page_token=None):
        list_response = self.big_query.list_tables(project_id, dataset_id,
                                                   page_token=page_token)
        if 'tables' in list_response:
            table_id_list = [table['tableReference']['tableId']
                             for table in list_response['tables']]
            tasks = self.create_table_tasks(project_id, dataset_id,
                                            table_id_list)
            Tasks.schedule(queue_name='gcp-metadata-tables', tasks=tasks)
        else:
            logging.info("Dataset %s.%s is empty", project_id, dataset_id)
            return

        if 'nextPageToken' in list_response:
            next_task = Task(
                method='GET',
                url='/bigQuery/project/%s/dataset/%s?pageToken=%s' %
                (project_id, dataset_id, list_response['nextPageToken']))
            Tasks.schedule(queue_name='gcp-metadata', tasks=[next_task])
        else:
            logging.info("There is no more tables in this dataset")

    def stream_table_metadata(self, project_id, dataset_id, table_id):
        table = self.big_query.get_table(project_id, dataset_id, table_id,
                                         log_table=False)
        if table:
            table_metadata = BigQueryTableMetadata(table)
            partitions = []
            if table_metadata.is_daily_partitioned():
                partitions = self.big_query.\
                    list_table_partitions(project_id, dataset_id, table_id)
            self.big_query.stream_stats(table_metadata, partitions)

    @staticmethod
    def create_project_tasks(project_id_list):
        for project_id in project_id_list:
            yield Task(method='GET',
                       url='/bigQuery/project/%s' % project_id)

    @staticmethod
    def create_dataset_tasks(project_id, dataset_id_list):
        for dataset_id in dataset_id_list:
            yield Task(method='GET',
                       url='/bigQuery/project/%s/dataset/%s'
                       % (project_id, dataset_id))

    @staticmethod
    def create_table_tasks(project_id, dataset_id, table_id_list):
        for table_id in table_id_list:
            yield Task(method='GET',
                       url='/bigQuery/project/%s/dataset/%s/table/%s'
                       % (project_id, dataset_id, table_id))
