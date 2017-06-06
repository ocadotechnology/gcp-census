class TableReference(object):

    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def get_project_id(self):
        return self.project_id

    def get_dataset_id(self):
        return self.dataset_id

    def get_table_id(self):
        return self.table_id

    def __str__(self):
        return '{0}:{1}.{2}'.format(self.project_id, self.dataset_id,
                                    self.table_id)
