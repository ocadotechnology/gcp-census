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
