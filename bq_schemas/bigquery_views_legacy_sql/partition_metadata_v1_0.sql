-- This view aggregates all partition metadata from last 2 days and deduplicates it based on partition reference.
-- Deleted table/partition can be returned by this query up to 2 days.
SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type FROM (
SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type,
row_number() OVER (PARTITION BY projectId, datasetId, tableId, partitionId ORDER BY snapshotTime DESC) AS rownum,
FROM [bigquery.partition_metadata_v1_0]
WHERE
_PARTITIONTIME BETWEEN TIMESTAMP(UTC_USEC_TO_DAY(NOW() - 24 * 60 * 60 * 1000000))
  AND TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP())))
WHERE rownum = 1