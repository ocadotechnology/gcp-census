from gcp_census.bigquery.row import Row
from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1
from gcp_census.bigquery.transformers.table_metadata_v1_0 import \
    TableMetadataV1_0
import datetime


class BigQueryTableStreamer(object):
    def __init__(self, big_query):
        self.big_query = big_query

    def stream_metadata(self, table, table_partitions):
        self.__stream(TableMetadataV0_1(table, table_partitions))
        self.__stream(TableMetadataV1_0(table))

    def __stream(self, transformer):
        row = Row(dataset_id="bigquery",
                  table_id=transformer.target_table_id,
                  insert_id=self.__create_insert_id(transformer.table,
                                                    transformer.target_table_id),
                  data=transformer.transform())
        self.big_query.stream_stats(row)

    @staticmethod
    def __create_insert_id(table, table_id):
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        insert_id = "{}/{}/{}/{}/{}".format(
            table_id,
            table['tableReference']['projectId'],
            table['tableReference']['datasetId'],
            table['tableReference']['tableId'],
            date)
        return insert_id
