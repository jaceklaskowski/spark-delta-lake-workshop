# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # PySpark
# MAGIC 
# MAGIC by Jacek Laskowski (jacek@japila.pl)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This exercise prepares you for some data processing, making your data cleaner for statistics and ML.
# MAGIC 
# MAGIC Load a CVS file and for every string field UPPER-CASE'd it.
# MAGIC 
# MAGIC **Note** By default loading a CVS file marks all fields as of `string` type so you should be safe to assume that you can just apply your `upper` standard function to every column.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC data = spark.range(5).withColumn('rand', rand(seed=42) * 3).withColumn('city', lit("Stockholm"))

# COMMAND ----------

display(data)

# COMMAND ----------

df = data
for c in data.columns:
    df = df.withColumn(f'{c}_UPPER', upper(c))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### EXTRA
# MAGIC 
# MAGIC Do me a favour and write the query so if we started with 3 columns I want to end up with 3 upper'ed columns.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC A much harder exercise is to apply a function based on field types, e.g.
# MAGIC 
# MAGIC * `upper` to `string` columns
# MAGIC * `% 2` to numeric columns

# COMMAND ----------

my_df = data
for k, v in data.dtypes:
    if (v == 'string'):
        my_df = my_df.withColumn(f'{k}', upper(k))
    elif (v in ['bigint', 'double']):
        my_df = my_df.withColumn(f'{k}', col(k) % 2)
display(my_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## FIXME
# MAGIC 
# MAGIC 1. Prepare a CVS file so people don't have to

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Some Ramblings about Spark MLlib

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
df = df.withColumn("features", lit("anton"))
lrModel = lr.fit(df)

# COMMAND ----------


