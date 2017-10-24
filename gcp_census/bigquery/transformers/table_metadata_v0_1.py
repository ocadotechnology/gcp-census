from gcp_census.bigquery.row import Row
import json
import time
import datetime


class TableMetadataV0_1(object):
    def __init__(self, table_metadata, partitions=None):
        if partitions is None:
            partitions = []
        self.table_metadata = table_metadata
        self.partitions = partitions

    def transform(self):
        table_dict = self.table_metadata.big_query_table

        return Row(dataset_id="bigquery",
                   table_id="TableMetadataV0_1",
                   insert_id=self.__create_insert_id(table_dict),
                   data=self.__create_data(table_dict, self.partitions))

    @staticmethod
    def __create_insert_id(table_dict):
        partition = datetime.datetime.now().strftime("%Y%m%d")
        insert_id = "{0}/{1}/{2}/{3}".format(
            table_dict['tableReference']['projectId'],
            table_dict['tableReference']['datasetId'],
            table_dict['tableReference']['tableId'],
            partition)
        return insert_id

    @staticmethod
    def __create_data(table_dict, partitions):
        return {
            'snapshotTime': time.time(),
            'projectId': table_dict['tableReference']['projectId'],
            'datasetId': table_dict['tableReference']['datasetId'],
            'tableId': table_dict['tableReference']['tableId'],
            'creationTime': float(table_dict['creationTime']) / 1000.0,
            'lastModifiedTime': float(table_dict['lastModifiedTime'])
                                / 1000.0,
            'partition': partitions,
            'location': table_dict['location']
            if 'location' in table_dict else None,
            'numBytes': table_dict['numBytes'],
            'numLongTermBytes': table_dict['numLongTermBytes'],
            'numRows': table_dict['numRows'],
            'timePartitioning': table_dict[
                'timePartitioning'] if 'timePartitioning'
                                       in table_dict else None,
            'type': table_dict['type'],
            'json': json.dumps(table_dict),
            'description': table_dict['description']
            if 'description' in table_dict else None,
            'labels': [{'key': key, 'value': value} for key, value
                       in table_dict['labels'].iteritems()]
            if 'labels' in table_dict else []
        }
