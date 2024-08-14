# Databricks notebook source
# MAGIC %md
# MAGIC - Write a pyspark program to find out the cumulative purchases of each product over time

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

schema = StructType([
 StructField("order_id", IntegerType(), True),
 StructField("product_type", StringType(), True),
 StructField("quantity", IntegerType(), True),
 StructField("order_date", StringType(), True),
])
 
data = [
 (213824, 'printer', 20, "2022-06-27 "),
 (212312, 'hair dryer', 5, "2022-06-28 "),
 (132842, 'printer', 18, "2022-06-28 "),
 (284730, 'standing lamp', 8, "2022-07-05 ")
]

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Find out cumulative purchases of each product over time
# MAGIC |order_id|product_type|quantity|order_date|cumm_product_qty|
# MAGIC |--------|------------|--------|----------|----------------|
# MAGIC |212312|hair dryer|5|2022-06-28|5|
# MAGIC |213824|printer|20|2022-06-27|20|
# MAGIC |132842|printer|18|2022-06-28|38|
# MAGIC |284730|standing lamp|8|2022-07-05|8|

# COMMAND ----------

window_spec = Window.partitionBy("product_type").orderBy("order_date").rowsBetween(Window.unboundedPreceding, 0)
res_df = (
    df.withColumn("cumm_product_qty", sum("quantity").over(window_spec))
)
display(res_df)
