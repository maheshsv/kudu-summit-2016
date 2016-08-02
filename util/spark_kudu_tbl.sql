CREATE EXTERNAL TABLE `spark_kudu_tbl` (
`name` STRING,
`age` INT,
`city` STRING
)
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'spark_kudu_tbl',
  'kudu.master_addresses' = 'ip-10-13-4-52.ec2.internal:7051',
  'kudu.key_columns' = 'name'
);
