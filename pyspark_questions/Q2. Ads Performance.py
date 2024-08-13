# Databricks notebook source
# MAGIC %md
# MAGIC - Write an pyspark program to find the ctr of each Ad. Round ctr to 2 decimal points. Order the result table by ctr in descending order and by ad_id in ascending order in case of a tie
# MAGIC - Note: Ctr = Clicked/(Clicked+Viewed)

# COMMAND ----------

schema=StructType([
StructField('AD_ID',IntegerType(),True)
,StructField('USER_ID',IntegerType(),True)
,StructField('ACTION',StringType(),True)
])
 
data = [
(1, 1, 'Clicked'),
(2, 2, 'Clicked'),
(3, 3, 'Viewed'),
(5, 5, 'Ignored'),
(1, 7, 'Ignored'),
(2, 7, 'Viewed'),
(3, 5, 'Clicked'),
(1, 4, 'Viewed'),
(2, 11, 'Viewed'),
(1, 2, 'Clicked')
]

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Implement a PYSPARK program to find CTR of each Ad.
# MAGIC |AD_ID|Ctr|
# MAGIC |-----|---|
# MAGIC |1|0.67|
# MAGIC |3|0.5|
# MAGIC |2|0.33|
# MAGIC |5|null|

# COMMAND ----------


