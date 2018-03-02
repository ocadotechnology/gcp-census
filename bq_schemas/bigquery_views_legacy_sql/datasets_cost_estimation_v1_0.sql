-- This view provides BigQuery datasets current estimated costs, which might differ to those provided by GCP billing
SELECT
  projectId,
  datasetId,
  ROUND(SUM(numBytes/(1024*1024*1024)), 3) AS totalStorageInGB,
  ROUND(SUM(numLongTermBytes/(1024*1024*1024)), 3) AS longTermStorageInGB,
  ROUND(SUM(numBytes/(1024*1024*1024))-SUM(numLongTermBytes/(1024*1024*1024)), 3) AS activeStorageInGB,
  ROUND( ((SUM(numBytes/(1024*1024*1024))-SUM(numLongTermBytes/(1024*1024*1024))) * 0.02 + (SUM(numLongTermBytes/(1024*1024*1024))) * 0.01), 3) AS estimatedMonthlyCostInUSD,
  SUM(numRows) AS numRows,
  COUNT(tableId) AS tableCount
FROM
  [bigquery_views_legacy_sql.table_metadata_v1_0]
GROUP BY
  projectId,
  datasetId