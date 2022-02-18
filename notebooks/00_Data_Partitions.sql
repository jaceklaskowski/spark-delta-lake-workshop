-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Data Partitions
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Motivation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. No control over the number of Spark partitions
-- MAGIC 1. Slow queries due to incorrect number of partitions
-- MAGIC     1. Number of partitions vs number of tasks
-- MAGIC     1. Number of partitions upon loading tables
-- MAGIC     1. Shuffles (Exchanges)
-- MAGIC 1. Table partitions in Hive
-- MAGIC 1. DataFrame operators
-- MAGIC     1. [repartition](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.repartition.html)
-- MAGIC     1. [coalesce](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.coalesce.html)
-- MAGIC 1. Hints
-- MAGIC     * `/*+ REPARTITION(42) */`
-- MAGIC 1. `spark.sql.suffle.partitions`
-- MAGIC     1. [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/configuration-properties/#spark.sql.shuffle.partitions)
-- MAGIC     1. [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Partitions, Tasks and CPU Cores

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Number of tasks: 10
-- MAGIC 1. Number of CPU cores: 2
-- MAGIC 
-- MAGIC How many CPU cycles to execute all the tasks? 5 ofc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC What about the size of the data to be processed?
-- MAGIC 
-- MAGIC Data size: 10g (if distributed evenly)
-- MAGIC 
-- MAGIC Executor memory: 1g
