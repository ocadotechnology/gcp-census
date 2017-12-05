-- This view aggregates all data from last 3 days and deduplicates it based on table reference.
-- Deleted table can be returned by this query up to 3 days.
SELECT * FROM (
SELECT projectId, datasetId, tableId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type,  timePartitioning.type AS partitioningType,
row_number() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum,
COUNT(partition.partitionId) WITHIN RECORD AS partitionCount
FROM [bigquery.table_metadata_v0_1]
WHERE
_PARTITIONTIME BETWEEN TIMESTAMP(UTC_USEC_TO_DAY(NOW() - 48 * 60 * 60 * 1000000))
  AND TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP())))
WHERE rownum = 1