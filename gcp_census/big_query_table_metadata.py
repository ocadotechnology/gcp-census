import logging
from types import NoneType


class BigQueryTableMetadata(object):
    def __init__(self, big_query_table):
        assert isinstance(big_query_table, (dict, NoneType))
        self.table_metadata = big_query_table

    def is_daily_partitioned(self):
        if self.table_metadata and 'timePartitioning' in self.table_metadata:
            table_id = self.table_metadata['tableReference']['tableId']
            if '$' in table_id:
                return False
            time_partitioning = self.table_metadata['timePartitioning']
            if 'type' in time_partitioning:
                type_of_partitioning = time_partitioning['type']
                if type_of_partitioning == 'DAY':
                    return True
            logging.error("Table metadata has different structure than "
                          "expected: %s", self.table_metadata)

        return False

