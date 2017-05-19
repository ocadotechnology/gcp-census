import os


def content(filename):
    if not os.path.exists(filename):
        raise Exception("File not found: {0}".format(filename))
    with open(filename, 'r') as f:
        return f.read()
