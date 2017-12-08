#standardSQL
-- This view aggregates all partition metadata from last 2 days and deduplicates it based on partition reference.
-- Deleted table/partition can be returned by this query up to 2 days.
SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type FROM (
SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type,
row_number() OVER (PARTITION BY projectId, datasetId, tableId, partitionId ORDER BY snapshotTime DESC) AS rownum
FROM `$PROJECT_ID.bigquery.partition_metadata_v1_0`
WHERE
_PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
  AND CURRENT_TIMESTAMP())
WHERE rownum = 1
