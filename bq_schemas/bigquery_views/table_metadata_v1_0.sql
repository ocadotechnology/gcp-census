#standardSQL
-- This view aggregates all table metadata from last 3 days and deduplicates it based on table reference.
-- Deleted table can be returned by this query up to 3 days.
SELECT projectId, datasetId, tableId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type FROM (
SELECT projectId, datasetId, tableId, creationTime, lastModifiedTime, location, numBytes, numLongTermBytes, numRows, type,
row_number() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
FROM `$PROJECT_ID.bigquery.table_metadata_v1_0`
WHERE
_PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
  AND CURRENT_TIMESTAMP())
WHERE rownum = 1