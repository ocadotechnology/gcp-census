import os
import json


def content(filename):
    if not os.path.exists(filename):
        raise Exception("File not found: {0}".format(filename))
    with open(filename, 'r') as f:
        return f.read()


def get_body_from_http_request(call):
    payload = call[1][2]
    return json.loads(payload) if payload else None


def create_minimal_table_dict():
    return {
        'tableReference': {
            'projectId': 'dev-manager',
            'datasetId': 'crm_raw',
            'tableId': 'account_1_0_0$20150603'
        },
        'schema': {
            'fields': [
                {
                    'name': 'transaction_id',
                    'type': 'INTEGER'
                },
                {
                    'name': 'transaction_date',
                    'type': 'DATE'
                }
            ]
        },
        'numBytes': '421940',
        'numLongTermBytes': '421940',
        'numRows': '1445',
        'creationTime': '1468414660572',
        'lastModifiedTime': '1468414660572',
        'type': 'TABLE',
    }
