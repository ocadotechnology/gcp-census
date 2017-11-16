class Table(object):
    def __init__(self, group, name, json_dict):
        self.group = group
        self.name = name
        self.json_dict = json_dict

    def __str__(self):
        return "Table(group={}, name={})".format(self.group, self.name)
