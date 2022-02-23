# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Delta Lake
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC by Jacek Laskowski (jacek@japila.pl)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Welcome
# MAGIC 
# MAGIC This is the last module (out of 4) to teach you how to use and think like a Spark SQL and Delta Lake pro.
# MAGIC 
# MAGIC 1 module takes 1,5h (2 x 45 mins with no break in-between)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schedule
# MAGIC 
# MAGIC ### Module 1. The Essentials of Spark SQL
# MAGIC 
# MAGIC * Part 1
# MAGIC   * Databricks Platform
# MAGIC   * Loading and Saving Datasets
# MAGIC * Part 2
# MAGIC   * Basic Transformations
# MAGIC   * Web UI
# MAGIC   
# MAGIC ### Module 2. Intermediate Spark SQL
# MAGIC 
# MAGIC * Part 1
# MAGIC   * Aggregations and Joins
# MAGIC * Part 2
# MAGIC   * Data Sources
# MAGIC   * Loading Datasets from Cloud Storage
# MAGIC 
# MAGIC ### Module 3. Advanced Spark SQL
# MAGIC 
# MAGIC * Part 1
# MAGIC   * Windowed Aggregation
# MAGIC * Part 2
# MAGIC   * Caching and Persistence
# MAGIC   * The Internals of Structured Query Execution
# MAGIC 
# MAGIC ### Module 4. Delta Lake (this notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Learning Resources
# MAGIC 
# MAGIC The recommended learning resources (for reading and watching) to get better equipped for the Spark Enablement series:
# MAGIC 
# MAGIC * [Apache Spark](https://spark.apache.org)
# MAGIC * [Delta Lake](https://delta.io)
# MAGIC * [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/)
# MAGIC * [Ensuring Consistency with ACID Transactions with Delta Lake (Loan Risk Data)](https://pages.databricks.com/rs/094-YMS-629/images/01-Delta%20Lake%20Workshop%20-%20Delta%20Lake%20Primer.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta Lake
# MAGIC 
# MAGIC [Slides](https://docs.google.com/presentation/d/1bkxEGDKYZoMbk7Cit8yZ5QZu6dgJybLyAxoot_VCcCY/edit?usp=sharing)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Introduction
# MAGIC 
# MAGIC 1. [Overview](https://books.japila.pl/delta-lake-internals/overview/)
# MAGIC 1. [Installation](https://books.japila.pl/delta-lake-internals/installation/)
# MAGIC 1. Optimization layer on top of a blob storage for reliability (ACID compliance) and low latency of streaming and batch data pipelines
# MAGIC     * Eventually consistent
# MAGIC     * Pretends to be a file system
# MAGIC 1. In short, the `delta` format is `parquet` with a transaction log (`_delta_log` directory) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Default File Format (Databricks)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.sessionState.conf.defaultDataSourceName

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.sources.default

# COMMAND ----------

spark.conf.get('spark.sql.sources.default')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CONVERT TO DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Convert existing parquet tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS demo_table

# COMMAND ----------

sql("DROP TABLE IF EXISTS demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rand.html

# COMMAND ----------

from pyspark.sql.functions import *

data = spark.range(5).withColumn('rand', rand(seed=42) * 3)
data.write.format("parquet").mode("overwrite").saveAsTable("demo_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC EXTENDED demo_table

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's use [CONVERT TO DELTA](https://books.japila.pl/delta-lake-internals/sql/#convert-to-delta) SQL command.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA demo_table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC EXTENDED demo_table

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_table

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_table/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DeltaTable.convertToDelta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. https://docs.delta.io/latest/quick-start.html

# COMMAND ----------

from delta.tables import *
DeltaTable.convertToDelta(identifier='demo_table', sparkSession=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### From Scratch

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE delta_demo
# MAGIC USING delta
# MAGIC AS VALUES (0, 'zero'), (1, 'one'), (2, 'two') t(id, name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC EXTENDED delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use [DESCRIBE DETAIL](https://books.japila.pl/delta-lake-internals/sql/#describe-detail) that comes with Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL delta_demo

# COMMAND ----------

display(spark.table("delta_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transaction Log

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/delta_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs head /user/hive/warehouse/delta_demo/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Writing to Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's add new rows to the delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO delta_demo VALUES
# MAGIC   (3, 'three'),
# MAGIC   (4, 'four')

# COMMAND ----------

display(spark.table('delta_demo').sort('id'))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE delta_demo VALUES
# MAGIC     ('1', 'jeden'),
# MAGIC     ('2', 'dwa');

# COMMAND ----------

display(spark.table('delta_demo'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's have a look at the transaction log.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/delta_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs head /user/hive/warehouse/delta_demo/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %fs head /user/hive/warehouse/delta_demo/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/delta_demo/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

display(spark.read.format("parquet").load("dbfs:/user/hive/warehouse/delta_demo/"))

# COMMAND ----------

# spark.range(5).write.format("parquet").save("dbfs:/user/hive/warehouse/delta_demo/")

# COMMAND ----------

display(spark.read.load("dbfs:/user/hive/warehouse/delta_demo/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table
# MAGIC DELETE FROM delta_demo WHERE id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Delta Lake table
# MAGIC UPDATE delta_demo SET name = 'deux' WHERE id = '2';
# MAGIC 
# MAGIC SELECT * FROM delta_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MERGE INTO

# COMMAND ----------

# MAGIC %md [Demo: Merge Operation](https://books.japila.pl/delta-lake-internals/demo/merge-operation/)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As you modify a Delta table, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### VERSION AS OF

# COMMAND ----------

display(spark.table('delta_demo'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_demo VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_demo VERSION AS OF 1

# COMMAND ----------

display(spark.read.option('versionAsOf', 1).table('delta_demo'))

# COMMAND ----------


