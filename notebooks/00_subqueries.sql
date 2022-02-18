-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Subqueries
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Subqueries (`WHERE` clause) and nested-loop joins
-- MAGIC 
-- MAGIC     ```
-- MAGIC     SELECT
-- MAGIC       x,
-- MAGIC       y,
-- MAGIC       z
-- MAGIC     FROM very_large_table_1
-- MAGIC     WHERE date IN (
-- MAGIC       SELECT date
-- MAGIC       FROM table_with_dates_also_quite_big
-- MAGIC       WHERE a = 42
-- MAGIC     )
-- MAGIC     ```
-- MAGIC 
-- MAGIC 1. Idealnie, w takich sytuacjach powinniśmy po prostu skorzystać z INNER JOIN'a. Fajnie byłoby podkreślic dlaczego jest to bardziej efektywny podejście i jak ogarnąć w UI, że Spark korzysta z nested loop join'a.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This module uses SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Introduction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Subqueries](https://spark.apache.org/docs/3.2.1/sql-ref-syntax-qry-select-subqueries.html)
-- MAGIC     * The official documentation of Spark SQL
-- MAGIC     * Work in progress unfortunatelly
-- MAGIC 1. [Subqueries (Subquery Expressions)](https://books.japila.pl/spark-sql-internals/spark-sql-subqueries/)
-- MAGIC     * The Internals of Spark SQL
-- MAGIC     * Unfortunatelly, it's a work in progress, too

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Fortunatelly, we've got an official notebook from Databricks! Thank you, Databricks Inc.
-- MAGIC 
-- MAGIC Visit [Subqueries in Apache Spark 2.0](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html).

-- COMMAND ----------


