# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark Structured Streaming
# MAGIC 
# MAGIC by Jacek Laskowski (jacek@japila.pl)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Welcome
# MAGIC 
# MAGIC This is one of the modules of the Spark and Delta Lake workshop to teach you how to use and think like a Spark SQL and Delta Lake pro.
# MAGIC 
# MAGIC This Databricks notebook teaches you [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) with Python (as the default language) and some SQL magic. Enjoy!
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC <font size="1">It is supposed to take 50 mins (but past experience showed something different).</font>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Learning Resources
# MAGIC 
# MAGIC The recommended learning resources (for reading and watching) to get better equipped for the module:
# MAGIC 
# MAGIC 1. [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Module

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC Based on the [official documentation of Apache Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise
# MAGIC 
# MAGIC Write a Spark Structured Streaming application (using Python perhaps) that loads CSV files from a directory and print them out to `console`.
# MAGIC 
# MAGIC [Exercise: Streaming CSV Datasets](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-structured-streaming-exercise-Streaming-CSV-Datasets.html)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types._
# MAGIC val my_schema = new StructType()
# MAGIC val stream = spark.readStream.schema(my_schema).csv("csvs")
# MAGIC stream
# MAGIC   .withColumn("name", lit("Sebastian in Stockholm"))
# MAGIC   .writeStream.format("console")
# MAGIC   .option("truncate", false)
# MAGIC   .option("checkpointLocation", "/tmp/jonas")
# MAGIC   .start

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC 
# MAGIC val spark = SparkSession
# MAGIC   .builder
# MAGIC   .appName("StructuredNetworkWordCount")
# MAGIC   .getOrCreate()
# MAGIC   
# MAGIC import spark.implicits._

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming
# MAGIC 1. Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine.
# MAGIC 1. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive.
# MAGIC 1. Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc.
# MAGIC 1. The computation is executed on the same optimized Spark SQL engine.
# MAGIC 1. the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs
# MAGIC 1. Structured Streaming queries are processed using a micro-batch processing engine, which processes data streams as a series of small batch jobs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. The example is reading data from a Delta Lake table
# MAGIC 1. Every insert into the table is another micro-batch
# MAGIC     * Speaking more broadly, every data change is a micro-batch
# MAGIC     * When you load a Delta table as a stream source and use it in a streaming query, the query processes all of the data present in the table as well as any new data that arrives after the stream is started.
# MAGIC 1. Multiple simultaneous writes are supported (as separate transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SparkSession

# COMMAND ----------

# SparkSession instance has already been created in any Databricks notebook.
spark

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS demo_streams

# COMMAND ----------

# MAGIC %sql SHOW TABLES LIKE 'demo_streams'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE demo_streams (
# MAGIC   id INTEGER,
# MAGIC   name STRING
# MAGIC )
# MAGIC -- USING delta
# MAGIC -- it is not needed as this is the default data source format on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### spark.sql.sources.default

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SET spark.sql.sources.default

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.conf.get("spark.sql.sources.default")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Streaming Table
# MAGIC 
# MAGIC 1. [Table streaming reads and writes](https://docs.databricks.com/delta/delta-streaming.html)
# MAGIC 1. [Streaming Table APIs](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-table-apis)
# MAGIC 1. [Demo notebooks](https://docs.databricks.com/spark/latest/structured-streaming/demo-notebooks.html)
# MAGIC 1. [Structured Streaming DataFrames](https://docs.databricks.com/notebooks/visualizations/index.html#structured-streaming-dataframes)

# COMMAND ----------

# this is a brach query / Spark SQL
spark.read.table('demo_streams').display()

# COMMAND ----------

# this is a streaming query / Spark Structured Streaming
spark.readStream.table('demo_streams').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### INSERT INTOs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Let's have a look at the schema of this Delta Lake table
# MAGIC DESC TABLE demo_streams

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO demo_streams VALUES (0, 'zero')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO demo_streams VALUES
# MAGIC   (1, 'one'),
# MAGIC   (2, 'two')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Delta-specific SQL statement
# MAGIC -- https://docs.databricks.com/delta/delta-utility.html#delta-history
# MAGIC DESC HISTORY demo_streams

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop Streaming Queries

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM demo_streams

# COMMAND ----------

dbutils.notebook.exit(0)

# COMMAND ----------

inputNode = spark.readStream.format('delta').load('')
# some transformation happens here
someTransformation = inputNode
someTransformation.writeStream.format('delta').trigger('5 seconds').start

# COMMAND ----------

spark

# COMMAND ----------


