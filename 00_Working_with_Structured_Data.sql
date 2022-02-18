-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Working with Structured Data
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Welcome
-- MAGIC 
-- MAGIC This module takes around 1,5h (2 x 45 mins)
-- MAGIC 
-- MAGIC Schedule:
-- MAGIC 1. 1pm - 1:50pm
-- MAGIC 1. 10' break
-- MAGIC 1. 2pm - 3:05pm
-- MAGIC 1. 10' break
-- MAGIC 1. Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Development Environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Azure Databricks](https://portal.azure.com/)
-- MAGIC 2. `spark-sql` (a pure SQL environment)
-- MAGIC 3. `spark-shell` (Scala REPL)
-- MAGIC 1. `pyspark` (Python CLI for Spark)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Databricks Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Databricks datasets](https://docs.databricks.com/data/databricks-datasets.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls('/databricks-datasets'))

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /databricks-datasets

-- COMMAND ----------

SELECT * FROM text.`dbfs:/databricks-datasets/README.md`

-- COMMAND ----------

SELECT * FROM text.`dbfs:/databricks-datasets/nyctaxi/sample/README.md`

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls dbfs:/databricks-datasets/nyctaxi/sample/json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(spark.read.json('dbfs:/databricks-datasets/nyctaxi/sample/json'))

-- COMMAND ----------

SELECT * FROM json.`dbfs:/databricks-datasets/nyctaxi/sample/json`

-- COMMAND ----------

DESCRIBE (SELECT * FROM json.`dbfs:/databricks-datasets/nyctaxi/sample/json`)

-- COMMAND ----------

SELECT * FROM VALUES (1), (2), (3) AS t(id)

-- COMMAND ----------

CREATE OR REPLACE TABLE my_demo_table AS VALUES (1), (2), (3) AS t(id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.catalog.listTables()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED my_demo_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. ANSI SQL mode GA
-- MAGIC     1. [One of the highlights of Spark 3.2.0](https://spark.apache.org/releases/spark-release-3-2-0.html)
-- MAGIC     1. [SPARK-35030](https://issues.apache.org/jira/browse/SPARK-35030)
-- MAGIC 1. [SQL Parsing Framework](https://books.japila.pl/spark-sql-internals/sql/) (The Internals of Spark SQL)
-- MAGIC 1. [SqlBase.g4](https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4) (Apache Spark)
-- MAGIC     1. Review [the changes](https://github.com/apache/spark/commits/v3.2.1/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4) to know when a particular statement was introduced or perhaps fixed

-- COMMAND ----------

-- SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t1.a = t2.c)
-- https://github.com/apache/spark/commit/f49bf1a072dd31b74503bbc50b3ed4d7df050402
SELECT * FROM VALUES (1), (2), (3) AS t1(a), LATERAL (SELECT * FROM VALUES (1, "one"), (2, "two"), (3, "three") AS t2(c, d) WHERE t1.a = t2.c)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Time: 3:05pm (Poland time)
-- MAGIC 
-- MAGIC Break: 10' --> 3:15pm

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [split function with variable delimiter per row](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/split-function-with-variable-delimiter-per-row.html)
-- MAGIC 1. [Using upper Standard Function](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-upper-Standard-Function.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercise 1 (split)
-- MAGIC 
-- MAGIC Time: 3:25pm --> 3:35pm

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dept = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/jacek@japila.pl/dept.csv").toDF("VALUES", "Delimiter")
-- MAGIC display(dept)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dept.createOrReplaceTempView("dept")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(spark.sql("SELECT *, split(values, delimiter) FROM dept"))

-- COMMAND ----------

SELECT *, split(values, delimiter) AS split_values FROM dept

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.split.html#pyspark.sql.functions.split
-- MAGIC from pyspark.sql import functions as F
-- MAGIC # The following won't work
-- MAGIC # split_values = F.split(dept["values"], pattern = dept["delimiter"])
-- MAGIC 
-- MAGIC # This one will
-- MAGIC # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.expr.html#pyspark.sql.functions.expr
-- MAGIC split_values = F.expr("split(vAlUeS, delimiter)")
-- MAGIC 
-- MAGIC # DataFrame.withColumn
-- MAGIC # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html
-- MAGIC solution = dept.withColumn('split_values', split_values)
-- MAGIC display(solution)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Extra

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC solution.createOrReplaceTempView('solution')

-- COMMAND ----------

-- query that removes empty tokens
-- tokens? Yeah...these elements of the split_values array
SELECT values, delimiter, array_remove(split_values, '') AS split_values FROM solution

-- COMMAND ----------

SELECT * FROM dept

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v1 AS VALUES ("1$", "$") AS v1(values, delimiter)

-- COMMAND ----------

desc v1

-- COMMAND ----------

SELECT * FROM v1

-- COMMAND ----------

SELECT *, split(values, concat('\\', delimiter)) AS split_values FROM v1

-- COMMAND ----------

SELECT concat('\\', delimiter) FROM v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Break at 4:25pm --> 4:40pm (Poland time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Exercise time: 4:45pm --> 4:55pm

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/jacek@japila.pl/cities-1.csv")
-- MAGIC      .withColumn("upper_name", F.upper(F.col('name')))
-- MAGIC      .withColumn("initcap_name", F.initcap(F.col('name'))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Done at 5pm Poland time
