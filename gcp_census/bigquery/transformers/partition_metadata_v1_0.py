from gcp_census.bigquery.transformers.table_metadata_v1_0 import \
    TableMetadataV1_0


class PartitionMetadataV1_0(TableMetadataV1_0):
    def __init__(self, table_metadata):
        super(PartitionMetadataV1_0, self).__init__(table_metadata)
        self.table_metadata = table_metadata
        self.target_table_id = 'table_metadata_v1_0'

    def transform(self):
        data = super(PartitionMetadataV1_0, self).transform()
        data['partitionId'] = self.table_metadata.get_partition_id()
        return data
