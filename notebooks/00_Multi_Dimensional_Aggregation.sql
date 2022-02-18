-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Multi-Dimensional Aggregation
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Motivation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Multiple `groupBy`s `union`ed vs multi-dimensional aggregation
-- MAGIC 
-- MAGIC     ```sql
-- MAGIC     INSERT OVERWRITE TABLE ....
-- MAGIC     SELECT
-- MAGIC       a, b, c
-- MAGIC     FROM (
-- MAGIC       SELECT a, 'All' AS b, SUM(c)
-- MAGIC       FROM table_1
-- MAGIC       GROUP BY a
-- MAGIC       UNION ALL
-- MAGIC       SELECT 'All' AS a, b, SUM(c)
-- MAGIC       FROM table_1
-- MAGIC       GROUP BY b
-- MAGIC       UNION ALL
-- MAGIC       ...
-- MAGIC 
-- MAGIC     )
-- MAGIC     ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Introduction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Slides](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-multi-dimensional-aggregation.html#/home)
-- MAGIC 1. [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/multi-dimensional-aggregation/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Demo
-- MAGIC 
-- MAGIC [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/multi-dimensional-aggregation/#demo)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Using rollup Operator for Total and Average Salaries by Department and Company-Wide](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-rollup-Operator-for-Total-and-Average-Salaries-by-Department-and-Company-Wide.html)
