from pyspark.sql import SparkSession
from cdf_utils import get_last_processed_version, update_last_processed_version, get_latest_changes

spark = SparkSession.builder.getOrCreate()

source_table = "lseg_lh_silver_ns.Silver_StockTrades"
target_table = "lseg_lh_gold.dbo.Gold_StockTrades"
metadata_table = "lseg_lh_silver_ns.etl_metadata"
table_name = "Silver_StockTrades"
key_col = "Exchange"

# 1. Get the last processed version
last_version = get_last_processed_version(spark, metadata_table, table_name)

# 2. Get the latest changes
get_latest_changes(spark, source_table, last_version, key_col)

# 3. Merge into Gold
spark.sql(f"""
    MERGE INTO {target_table} t
    USING latest_changes s
    ON s.{key_col} = t.{key_col}
    WHEN MATCHED AND s._change_type = 'update_postimage' THEN
        UPDATE SET ExecutionRate = s.NumTradesExecuted / s.TotalOrders
    WHEN MATCHED AND s._change_type = 'delete' THEN
        UPDATE SET DeletedFlag = 'Y'
    WHEN NOT MATCHED THEN
        INSERT (Exchange, ExecutionRate, DeletedFlag)
        VALUES (s.{key_col}, s.NumTradesExecuted / s.TotalOrders, 'N')
""")

# 4. Update metadata
new_version = spark.sql(f"DESCRIBE HISTORY {source_table}").agg({"version": "max"}).collect()[0][0]
update_last_processed_version(spark, metadata_table, table_name, new_version)