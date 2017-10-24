import json
import time


class TableMetadataV0_1(object):
    def __init__(self, table, partitions=None):
        self.table = table
        self.partitions = [] if partitions is None else partitions
        self.target_table_id = 'table_metadata_v0_1'

    def transform(self):
        return {
            'snapshotTime': time.time(),
            'projectId': self.table['tableReference']['projectId'],
            'datasetId': self.table['tableReference']['datasetId'],
            'tableId': self.table['tableReference']['tableId'],
            'creationTime': float(self.table['creationTime']) / 1000.0,
            'lastModifiedTime': float(self.table['lastModifiedTime'])
                                / 1000.0,
            'partition': self.partitions,
            'location': self.table['location']
            if 'location' in self.table else None,
            'numBytes': self.table['numBytes'],
            'numLongTermBytes': self.table['numLongTermBytes'],
            'numRows': self.table['numRows'],
            'timePartitioning': self.table[
                'timePartitioning'] if 'timePartitioning'
                                       in self.table else None,
            'type': self.table['type'],
            'json': json.dumps(self.table),
            'description': self.table['description']
            if 'description' in self.table else None,
            'labels': [{'key': key, 'value': value} for key, value
                       in self.table['labels'].iteritems()]
            if 'labels' in self.table else []
        }
