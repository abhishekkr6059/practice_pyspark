# Databricks notebook source
# MAGIC %md
# MAGIC - Write a solution to find the first login date for each player. Return the result table in any order.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

activity_schema = StructType([
 StructField("player_id", IntegerType(), True),
 StructField("device_id", IntegerType(), True),
 StructField("event_date", StringType(), True),
 StructField("games_played", IntegerType(), True)
])
 
activity_data = [
 (1, 2, '2016-03-01', 5),
 (1, 2, '2016-05-02', 6),
 (2, 3, '2017-06-25', 1),
 (3, 1, '2016-03-02', 0),
 (3, 4, '2018-07-03', 5)
]

# COMMAND ----------

activity_df = spark.createDataFrame(activity_data, schema=activity_schema)
display(activity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Find the first login date for each player
# MAGIC |player_id|first_login|
# MAGIC |---------|-----------|
# MAGIC |1|2016-03-01|
# MAGIC |2|2017-06-25|
# MAGIC |3|2016-03-02|

# COMMAND ----------

res_df = (
    activity_df.withColumn("rank", rank().over(Window.partitionBy("player_id").orderBy("event_date")))
    .filter(col("rank") == 1)
    .select("player_id",col("event_date").alias("first_login"))
)
display(res_df)
