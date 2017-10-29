import logging

from types import NoneType


class BigQueryTableMetadata(object):
    def __init__(self, table):
        assert isinstance(table, (dict, NoneType))
        self.table = table

    def is_daily_partitioned(self):
        if self.table and 'timePartitioning' in self.table:
            if self.is_partition():
                return False
            time_partitioning = self.table['timePartitioning']
            if 'type' in time_partitioning:
                type_of_partitioning = time_partitioning['type']
                if type_of_partitioning == 'DAY':
                    return True
            logging.error("Table metadata has different structure than "
                          "expected: %s", self.table)

        return False

    def is_partition(self):
        table_id = self.table['tableReference']['tableId']
        return '$' in table_id

    def get_partition_id(self):
        assert self.is_partition() == True
        table_id = self.table['tableReference']['tableId']
        return table_id.split('$')[1]

    def get_table_id(self):
        table_id = self.table['tableReference']['tableId']
        return table_id.split('$')[0]

