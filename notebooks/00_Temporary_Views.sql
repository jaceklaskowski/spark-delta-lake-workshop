-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Temporary Views
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This module uses (Scala or Python) and SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Motivation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Wiele z naszych procesów opiera się na SQLu i sekwencji transformacji na danych (m.in. żeby ten kod jakoś czytelnie zorganizwoać). Czesto każdy krok z takiej serii transformacji kończy się stworzeniem widoku.
-- MAGIC 1. Wszyscy początkujący zakładają, że stworzenie widoku materializuje dane, a kończy się to w praktyce wielokrotnym czytaniem danych źródłowych i spowolnieniem pipeline'u.
-- MAGIC 1. W tym kontekście można wspomnieć o użyciu cache / persist

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [How does createOrReplaceTempView work in Spark?](https://stackoverflow.com/q/44011846/1305344) (StackOverflow)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Demo: Understanding createOrReplaceTempView

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Caching and Persistence
-- MAGIC 
-- MAGIC [Slides](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-dataset-caching-and-persistence.html#/home)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q = spark.range(5)
-- MAGIC q.cache().count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q.unpersist()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q = spark.range(5).groupBy("id").count()
-- MAGIC display(q)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q.createOrReplaceTempView("groups")

-- COMMAND ----------

CACHE TABLE groups

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q.cache().count()
-- MAGIC q.cache().count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Is it possible that `cache`'ing at different times while running your Spark application could cache different blocks (_rows_)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Demo: Caching createOrReplaceTempView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC * Time: 1pm --> 2:20pm
-- MAGIC * Break time: 10'
