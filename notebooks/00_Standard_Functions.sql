-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Standard Functions
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This module uses SQL and Python.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Motivation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Where to find all the built-in standard functions in Spark SQL?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC A common use case is a terribly slow code with CROSS JOIN to fill out gaps in time series data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   dates.date,
-- MAGIC   COALESCE(t1.a, 'foo') AS a,
-- MAGIC   COALESCE(t1.b, 'bar') AS b
-- MAGIC FROM table_1 AS t1
-- MAGIC CROSS JOIN (
-- MAGIC   -- date generation
-- MAGIC ) AS dates
-- MAGIC ON 1 = 1
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Solution is to use `SEQUENCE` and `LATERAL VIEW EXPLODE`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   dates,
-- MAGIC   COALESCE(t1.a, 'foo') AS a,
-- MAGIC   COALESCE(t1.b, 'bar') AS b
-- MAGIC FROM table_1 AS t1
-- MAGIC LATERAL VIEW EXPLODE(SEQUENCE(START_DATE, END_DATE, INTERVAL 42 DAYS)) tmp AS dates
-- MAGIC ```

-- COMMAND ----------

SELECT SEQUENCE(to_date('2021-02-15'), to_date('2021-05-15'), INTERVAL 42 DAYS)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC q = spark.range(0, 1, 1, 200).select(F.expr("SEQUENCE(to_date('2021-02-15'), to_date('2021-05-15'), INTERVAL 42 DAYS)").alias("new_column"))
-- MAGIC display(q)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.selectExpr.html
-- MAGIC number_of_files = 200000
-- MAGIC dataset = spark.range(0, 1, 1, number_of_files)
-- MAGIC time_series = (dataset
-- MAGIC   .selectExpr(
-- MAGIC     "SEQUENCE(to_date('2021-02-15'), to_date('2021-05-15'), INTERVAL 42 DAYS) AS time_series"))
-- MAGIC display(time_series)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(time_series.withColumn("exploded", F.explode('time_series')))

-- COMMAND ----------

SELECT
  dates
--FROM VALUES (1)
LATERAL VIEW EXPLODE(SEQUENCE(to_date('2021-02-15'), to_date('2021-05-15'), INTERVAL 42 DAYS)) tmp AS dates

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC spark.range(1).queryExecution.toRdd.getNumPartitions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC sc.defaultParallelism

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC I've got 32 CPU cores.
-- MAGIC 
-- MAGIC Q: How many CPU "cycles" do I have to use to execute 64 tasks? 2!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## SHOW FUNCTIONS

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------

DESC FUNCTION sum

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.catalog.listFunctions()

-- COMMAND ----------

DESC FUNCTION EXTENDED sum

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Remember [SqlBase.g4](https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4)?

-- COMMAND ----------

SHOW FUNCTIONS 'su*'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Built-in Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [The official documentation of Spark SQL](https://spark.apache.org/docs/3.2.1/api/sql/index.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Limiting collect_set Standard Function](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/limiting-collect_set-standard-function.html)
-- MAGIC 1. [Structs for column names and values](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/structs-for-column-names-and-values.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Exercise time: 6:40pm --> 6:50pm
-- MAGIC 1. 10 minutes left for doing this exercise together

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC input = spark.range(50).withColumn("key", F.col("id") % 5)
-- MAGIC display(input)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC all = input.groupBy("key").agg(F.collect_set("id").alias("all"))
-- MAGIC display(all)

-- COMMAND ----------

SHOW FUNCTIONS LIKE 'array*'

-- COMMAND ----------

DESC FUNCTION EXTENDED array_remove

-- COMMAND ----------

DESC FUNCTION EXTENDED array_position

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Use https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.slice.html
-- MAGIC display(all.withColumn('only_first_three', F.slice(all['all'], 1, 3)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Yay! Finished at 7pm exactly!
