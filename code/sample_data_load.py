from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Day 0 data
data_day0 = [("LSE_London", 100000, 120000),
             ("NASDAQ_NewYork", 200000, 250000),
             ("JPX_Tokyo", 150000, 170000)]
cols = ["Exchange", "NumTradesExecuted", "TotalOrders"]

df_day0 = spark.createDataFrame(data_day0, cols)
df_day0.write.format("delta").mode("overwrite").saveAsTable("lseg_lh_silver_ns.Silver_StockTrades")

# Gold Table Initial Load
from pyspark.sql.functions import col, lit

df_gold = df_day0.withColumn("ExecutionRate", col("NumTradesExecuted") / col("TotalOrders"))                  .withColumn("DeletedFlag", lit("N"))                  .drop("NumTradesExecuted", "TotalOrders")

df_gold.write.format("delta").mode("overwrite").save("abfss://FabricDemo@onelake.dfs.fabric.microsoft.com/lseg_lh_gold.Lakehouse/Tables/Gold_StockTrades")