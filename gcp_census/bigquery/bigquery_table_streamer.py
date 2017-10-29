import datetime

from gcp_census.bigquery.row import Row
from gcp_census.bigquery.transformers.table_metadata_v0_1 import \
    TableMetadataV0_1
from gcp_census.bigquery.transformers.table_metadata_v1_0 import \
    TableMetadataV1_0


class BigQueryTableStreamer(object):
    def __init__(self, big_query):
        self.big_query = big_query

    def stream_metadata(self, table_metadata, table_partitions):
        self.__stream(TableMetadataV0_1(table_metadata, table_partitions))
        self.__stream(TableMetadataV1_0(table_metadata))
        if table_metadata.is_partition():
            self.__stream(TableMetadataV1_0(table_metadata))

    def __stream(self, transformer):
        row = Row(dataset_id="bigquery",
                  table_id=transformer.target_table_id,
                  insert_id=self.__create_insert_id(transformer.table_metadata,
                                                    transformer.target_table_id),
                  data=transformer.transform())
        self.big_query.stream_stats(row)

    @staticmethod
    def __create_insert_id(table_metadata, target_table_id):
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        insert_id = "{}/{}/{}/{}/{}".format(
            target_table_id,
            table_metadata.table['tableReference']['projectId'],
            table_metadata.table['tableReference']['datasetId'],
            table_metadata.table['tableReference']['tableId'],
            date)
        return insert_id
