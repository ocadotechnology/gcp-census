import logging
from types import NoneType


class BigQueryTableMetadata(object):
    def __init__(self, big_query_table):
        assert isinstance(big_query_table, (dict, NoneType))
        self.big_query_table = big_query_table

    def is_daily_partitioned(self):
        if self.big_query_table and 'timePartitioning' in self.big_query_table:
            table_id = self.big_query_table['tableReference']['tableId']
            if '$' in table_id:
                return False
            time_partitioning = self.big_query_table['timePartitioning']
            if 'type' in time_partitioning:
                type_of_partitioning = time_partitioning['type']
                if type_of_partitioning == 'DAY':
                    return True
            logging.error("Table metadata has different structure than "
                          "expected: %s", self.big_query_table)

        return False

    def is_partition(self):
        table_id = self.big_query_table['tableReference']['tableId']
        return '$' in table_id

    def get_partition_id(self):
        assert self.is_partition() == True
        table_id = self.big_query_table['tableReference']['tableId']
        return table_id.split('$')[1]

    def get_table_id(self):
        table_id = self.big_query_table['tableReference']['tableId']
        return table_id.split('$')[0]

