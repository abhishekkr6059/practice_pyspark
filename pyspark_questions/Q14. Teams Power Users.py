# Databricks notebook source
# MAGIC %md
# MAGIC - Write a pyspark code to identify the top 2 Power Users who sent the highest number of messages on Microsoft Teams in August 2022. Output the results in descending order based on the count of the messages.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema = StructType([
 StructField("message_id", IntegerType(), True),
 StructField("sender_id", IntegerType(), True),
 StructField("receiver_id", IntegerType(), True),
 StructField("content", StringType(), True),
 StructField("sent_date", StringType(), True),
])
 
data = [
 (901, 3601, 4500, 'You up?', '2022-08-03 00:00:00'),
 (902, 4500, 3601, 'Only if you\'re buying', '2022-08-03 00:00:00'),
 (743, 3601, 8752, 'Let\'s take this offline', '2022-06-14 00:00:00'),
 (922, 3601, 4500, 'Get on the call', '2022-08-10 00:00:00'),
]

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Top 2 Power Users who sent the highest number of messages on Microsoft Teams in August 2022
# MAGIC |sender_id|messages_sent|
# MAGIC |---------|-------------|
# MAGIC |3601|2|
# MAGIC |4500|1|

# COMMAND ----------


