import logging

from datetime import datetime
from google.appengine.api.taskqueue import Task

from gcp_census.bigquery.bigquery_table_metadata import BigQueryTableMetadata
from gcp_census.bigquery.bigquery_table_streamer import BigQueryTableStreamer
from gcp_census.tasks import Tasks


class BigQueryTask(object):
    def __init__(self, big_query):
        self.big_query = big_query
        self.table_streamer = BigQueryTableStreamer(big_query)

    def schedule_task_for_each_project(self):
        tasks = self.create_project_tasks(self.big_query.list_project_ids())
        Tasks.schedule(queue_name='bigquery-list', tasks=tasks)

    def schedule_task_for_each_dataset(self, project_id):
        tasks = self.create_dataset_tasks(project_id, self.big_query.
                                          list_dataset_ids(project_id))
        Tasks.schedule(queue_name='bigquery-list', tasks=tasks)

    def schedule_task_for_each_table(self, project_id, dataset_id,
                                     page_token=None):
        list_response = self.big_query.list_tables(project_id, dataset_id,
                                                   page_token=page_token)
        if 'tables' in list_response:
            table_id_list = [table['tableReference']['tableId']
                             for table in list_response['tables']]
            tasks = self.create_table_tasks(project_id, dataset_id,
                                            table_id_list)
            Tasks.schedule(queue_name='bigquery-tables', tasks=tasks)
        else:
            logging.info("Dataset %s.%s is empty", project_id, dataset_id)
            return

        if 'nextPageToken' in list_response:
            url = '/bigQuery/project/%s/dataset/%s?pageToken=%s' % (
                project_id, dataset_id, list_response['nextPageToken'])
            task_name = '%s-%s-%s-%s' % (project_id, dataset_id,
                                         list_response['nextPageToken'],
                                         datetime.utcnow().strftime("%Y%m%d"))
            next_task = Task(
                method='GET',
                url=url,
                name=task_name)
            Tasks.schedule(queue_name='bigquery-list', tasks=[next_task])
        else:
            logging.info("There is no more tables in this dataset")

    def schedule_task_for_each_partition(self, project_id, dataset_id, table_id,
                                         partitions):
        tasks = self.create_partition_tasks(project_id, dataset_id, table_id,
                                            partitions)
        Tasks.schedule(queue_name='bigquery-tables', tasks=tasks)

    def stream_table_metadata(self, project_id, dataset_id, table_id):
        table = self.big_query.get_table(project_id, dataset_id, table_id)
        if table:
            table_metadata = BigQueryTableMetadata(table)
            partitions = []
            if table_metadata.is_daily_partitioned():
                partitions = self.big_query. \
                    list_table_partitions(project_id, dataset_id, table_id)
                self.schedule_task_for_each_partition(project_id,
                                                      dataset_id,
                                                      table_id,
                                                      partitions)
            self.table_streamer.stream_metadata(table_metadata, partitions)

    def create_partition_tasks(self, project_id, dataset_id, table_id,
                               partitions):
        table_id_list = ["{}${}".format(table_id, partition['partitionId'])
                         for partition in partitions]
        return self.create_table_tasks(project_id, dataset_id, table_id_list)

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
