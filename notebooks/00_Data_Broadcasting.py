# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Broadcast Variables / Data Broadcasting
# MAGIC 
# MAGIC by Jacek Laskowski (jacek@japila.pl)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This module uses (Scala or Python) and SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Data broadcasting
# MAGIC     1. [RDD Programming Guide](https://spark.apache.org/docs/3.2.1/rdd-programming-guide.html#broadcast-variables)
# MAGIC     1. [The Internals of Apache Spark](https://books.japila.pl/apache-spark-internals/broadcast-variables/)
# MAGIC 1. [Hints](https://spark.apache.org/docs/3.2.1/sql-ref-syntax-qry-select-hints.html) (The official documentation of Spark SQL)
# MAGIC 1. Broadcast joins
# MAGIC     1. [Join Strategy Hints for SQL Queries](https://spark.apache.org/docs/3.2.1/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)
# MAGIC     1. [spark.sql.autoBroadcastJoinThreshold](https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options)
# MAGIC     1. [spark.sql.autoBroadcastJoinThreshold](https://books.japila.pl/spark-sql-internals/configuration-properties/#spark.sql.autoBroadcastJoinThreshold) (The Internals of Spark SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Demo

# COMMAND ----------

lookupTable = range(1000)

lookupTable_bv = sc.broadcast(lookupTable)

# Let's assume we've got 2 executors
# The lookupTable is sent over the wire at most twice
# If it happens that 5 tasks are scheduled to be run on the same executor
# the lookupTable is going to be sent out to this executor just once
# And no other executor gets the lookupTable
fiveRowDataset = spark.range(5)
fiveRowDataset\
    .foreach(row => 
             # the body of this function is a Spark task
             # that runs on executors
             # distributed computation = task
             zip_code = row["zip_code"]
             kv = lookupTable_bv.value()
             city = kv[zip_code]
    )

# Think about network bandwidth


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SET spark.sql.autoBroadcastJoinThreshold=1m

# COMMAND ----------

spark.range(10e4).write.mode("overwrite").saveAsTable("t_10e4")
spark.range(10e8).write.mode("overwrite").saveAsTable("t_10e8")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val t_10e4 = spark.table("t_10e4")
# MAGIC val t_10e8 = spark.table("t_10e8")
# MAGIC val q = t_10e4.join(t_10e8, "id")
# MAGIC q.foreach(_ => ())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SET spark.sql.autoBroadcastJoinThreshold=-1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Current time: 6:50pm
