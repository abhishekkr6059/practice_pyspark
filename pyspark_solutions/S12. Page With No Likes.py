# Databricks notebook source
# MAGIC %md
# MAGIC - Write a pyspark code to return the IDs of the Facebook pages that have zero likes. The output should be sorted in ascending order based on the page IDs.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

pages_schema = StructType([
StructField("page_id", IntegerType(), True),
StructField("page_name", StringType(), True)
])

page_likes_schema = StructType([
StructField("user_id", IntegerType(), True),
StructField("page_id", IntegerType(), True),
StructField("liked_date", StringType(), True)
])

pages_data = [
(20001, 'SQL Solutions'),
(20045, 'Brain Exercises'),
(20701, 'Tips for Data Analysts'),
(20551, 'Tips for Data Engineer')
]

page_likes_data = [
(111, 20001, '2022-04-08 00:00:00'),
(121, 20045, '2022-03-12 00:00:00'),
(156, 20001, '2022-07-25 00:00:00')
]

# COMMAND ----------

pages_df = spark.createDataFrame(pages_data, schema=pages_schema)
display(pages_df)

# COMMAND ----------

page_likes_df = spark.createDataFrame(page_likes_data, schema=page_likes_schema)
display(page_likes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Return the IDs of the Facebook pages that have zero likes.
# MAGIC |page_id|
# MAGIC |-------|
# MAGIC |20551|
# MAGIC |20701|

# COMMAND ----------

res_df = (
    pages_df.join(page_likes_df, "page_id", "left_anti")
    .select("page_id")
    .orderBy("page_id")
)
display(res_df)
