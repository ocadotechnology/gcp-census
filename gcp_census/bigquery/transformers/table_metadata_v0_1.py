import json
import time


class TableMetadataV0_1(object):
    def __init__(self, table_metadata, partitions=None):
        self.table_metadata = table_metadata
        self.partitions = [] if partitions is None else partitions
        self.target_table_id = 'table_metadata_v0_1'

    def transform(self):
        table = self.table_metadata.table
        return {
            'snapshotTime': time.time(),
            'projectId': table['tableReference']['projectId'],
            'datasetId': table['tableReference']['datasetId'],
            'tableId': table['tableReference']['tableId'],
            'creationTime': float(table['creationTime']) / 1000.0,
            'lastModifiedTime': float(table['lastModifiedTime'])
                                / 1000.0,
            'partition': self.partitions,
            'location': table['location']
            if 'location' in table else None,
            'numBytes': table['numBytes'],
            'numLongTermBytes': table['numLongTermBytes'],
            'numRows': table['numRows'],
            'timePartitioning': table[
                'timePartitioning'] if 'timePartitioning'
                                       in table else None,
            'type': table['type'],
            'json': json.dumps(table),
            'description': table['description']
            if 'description' in table else None,
            'labels': [{'key': key, 'value': value} for key, value
                       in table['labels'].iteritems()]
            if 'labels' in table else []
        }
