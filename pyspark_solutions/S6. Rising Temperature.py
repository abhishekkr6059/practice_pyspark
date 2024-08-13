# Databricks notebook source
# MAGIC %md
# MAGIC - Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday). Return the result table in any order.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the "Weather" table
weather_schema = StructType([
StructField("id", IntegerType(), True),
StructField("recordDate", StringType(), True),
StructField("temperature", IntegerType(), True)
])
 
# Define data for the "Weather" table
weather_data = [
(1, '2015-01-01', 10),
(2, '2015-01-02', 25),
(3, '2015-01-03', 20),
(4, '2015-01-04', 30)
]

# COMMAND ----------

weather_df = spark.createDataFrame(weather_data, schema=weather_schema)
display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC -  find dates with higher temperatures compared to its previous date
# MAGIC |id|recordDate|
# MAGIC |--|----------|
# MAGIC |2|2015-01-02|
# MAGIC |4|2015-01-04|
