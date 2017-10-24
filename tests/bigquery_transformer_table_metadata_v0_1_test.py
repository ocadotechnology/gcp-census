import unittest

from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1


class TestTableMetadataV0_1(unittest.TestCase):
    def test_transforming_table_without_labels(self):
        # given
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'sit-atm-eu-datamanager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'sit-atm-eu-datamanager',
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
        data = TableMetadataV0_1(table).transform()

        # then
        self.assertEqual('account_1_0_0_20150603', data['tableId'])
        self.assertEqual(0, len(data['labels']))

    def test_transforming_table_with_labels(self):
        # given
        table = {
            'kind': 'bigquery#table',
            'etag': '\'smpMas70-D1-zV2oEH0ud6qY21c/MTQ2ODQxNDY2MDU3Mg\'',
            'id': 'sit-atm-eu-datamanager:crm_raw.account_1_0_0_20150603',
            'tableReference': {
                'projectId': 'sit-atm-eu-datamanager',
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
        data = TableMetadataV0_1(table).transform()

        # then
        self.assertEqual(3, len(data['labels']))
        self.assertEqual(data['labels'][0]['key'], 'key3')
        self.assertEqual(data['labels'][0]['value'], 'value3')
