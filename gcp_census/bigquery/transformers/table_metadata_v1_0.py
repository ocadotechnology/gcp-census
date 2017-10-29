import json
import time


class TableMetadataV1_0(object):
    def __init__(self, table_metadata):
        self.table_metadata = table_metadata
        self.target_table_id = 'table_metadata_v1_0'

    def transform(self):
        table = self.table_metadata.table
        return {
            'snapshotTime': time.time(),
            'projectId': table['tableReference']['projectId'],
            'datasetId': table['tableReference']['datasetId'],
            'tableId': self.table_metadata.get_table_id(),
            'creationTime': float(table['creationTime']) / 1000.0,
            'lastModifiedTime': float(table['lastModifiedTime'])
                                / 1000.0,
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
