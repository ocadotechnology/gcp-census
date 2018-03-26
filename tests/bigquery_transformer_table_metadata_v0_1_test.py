import unittest

import test_utils

from gcp_census.bigquery.bigquery_table_metadata import BigQueryTableMetadata
from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1


class TestTableMetadataV0_1(unittest.TestCase):
    def test_transforming_table_without_labels(self):
        # given
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'dev-manager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'dev-manager',
                'datasetId': 'crm_raw',
                'tableId': 'account_1_0_0_20150603'
            },
            "description": "secs\n\njhbhgvhgv\n\nlorem",
            'numBytes': '421940',
            'numLongTermBytes': '421940',
            'numRows': '1445',
            'creationTime': '1468414660572',
            'lastModifiedTime': '1468414660572',
            'type': 'TABLE',
            'location': 'US'
        }

        # when
        data = TableMetadataV0_1(BigQueryTableMetadata(table)).transform()

        # then
        self.assertEqual('account_1_0_0_20150603', data['tableId'])
        self.assertEqual(0, len(data['labels']))

    def test_transforming_table_with_labels(self):
        # given
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'dev-manager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'dev-manager',
                'datasetId': 'crm_raw',
                'tableId': 'account_1_0_0_20150603'
            },
            "description": "secs\n\njhbhgvhgv\n\nlorem",
            "labels": {
                "key1": "value1",
                "empty": "",
                "key3": "value3"
            },
            'numBytes': '421940',
            'numLongTermBytes': '421940',
            'numRows': '1445',
            'creationTime': '1468414660572',
            'lastModifiedTime': '1468414660572',
            'type': 'TABLE',
            'location': 'US'
        }

        # when
        data = TableMetadataV0_1(BigQueryTableMetadata(table)).transform()

        # then
        self.assertEqual(3, len(data['labels']))
        self.assertEqual(data['labels'][0]['key'], 'key3')
        self.assertEqual(data['labels'][0]['value'], 'value3')

    def test_should_ignore_timepartitioning_field(self):
        # given
        table = test_utils.create_minimal_table_dict()
        table['timePartitioning'] = {
            'type': 'DAY',
            'expirationMs': '259200000',
            'field': 'transaction_date'
        }

        # when
        data = TableMetadataV0_1(BigQueryTableMetadata(table)).transform()

        # then
        self.assertEqual('DAY', data['timePartitioning']['type'])
        self.assertEqual('259200000', data['timePartitioning']['expirationMs'])
        self.assertFalse('field' in data['timePartitioning'])

    def test_should_parse_timepartitioning_without_expiration_ms(self):
        # given
        table = test_utils.create_minimal_table_dict()
        table['timePartitioning'] = {
            'type': 'DAY',
        }

        # when
        data = TableMetadataV0_1(BigQueryTableMetadata(table)).transform()

        # then
        self.assertEqual('DAY', data['timePartitioning']['type'])
        self.assertFalse('expirationMs' in data['timePartitioning'])
