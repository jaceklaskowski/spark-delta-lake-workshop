-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Advanced Spark SQL
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Welcome
-- MAGIC 
-- MAGIC This is the third module (out of 4) to teach you how to use and think like a Spark SQL and Delta Lake pro.
-- MAGIC 
-- MAGIC 1 module takes 1,5h (2 x 45 mins with no break in-between)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Schedule
-- MAGIC 
-- MAGIC ### Module 1. The Essentials of Spark SQL
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Databricks Platform
-- MAGIC   * Loading and Saving Datasets
-- MAGIC * Part 2
-- MAGIC   * Basic Transformations
-- MAGIC   * Web UI
-- MAGIC   
-- MAGIC ### Module 2. Intermediate Spark SQL
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Aggregations and Joins
-- MAGIC * Part 2
-- MAGIC   * [Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
-- MAGIC   * Loading Datasets from Cloud Storage
-- MAGIC 
-- MAGIC ### Module 3. Advanced Spark SQL (this notebook)
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Windowed Aggregation
-- MAGIC * Part 2
-- MAGIC   * Caching and Persistence
-- MAGIC   * The Internals of Structured Query Execution
-- MAGIC 
-- MAGIC ### Module 4. Delta Lake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Learning Resources
-- MAGIC 
-- MAGIC The recommended learning resources (for reading and watching) to get better equipped for the Spark Enablement series:
-- MAGIC 
-- MAGIC * [Apache Spark](https://spark.apache.org)
-- MAGIC * [Delta Lake](https://delta.io)
-- MAGIC * [The Internals Online Books](https://books.japila.pl/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Windowed Aggregation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Window Aggregate Functions
-- MAGIC 
-- MAGIC 1. **Window Aggregate Functions** are standard functions that perform calculations over a group of records called a **window**
-- MAGIC 1. **Window** defines a logical group of rows that are in some relation to the current record
-- MAGIC 1. Generates a value for every row
-- MAGIC     * Unlike basic aggregations that generate at most the number of input rows
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-functions-windows.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Window Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Aggregate functions
-- MAGIC     * sum, avg, min, max, count, etc.
-- MAGIC     * User-Defined Aggregate Functions (UDAFs)
-- MAGIC 1. Ranking functions
-- MAGIC     * rank, dense_rank, percent_rank
-- MAGIC     * ntile
-- MAGIC     * row_number
-- MAGIC 1. Analytic functions
-- MAGIC     * cume_dist, lag, lead
-- MAGIC 1. Visit the PySpark documentation for [Standard Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Window Specification

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. **Window Specification** defines a window (logical view) that includes the rows that are in some relation to the current row (for every row)
-- MAGIC 1. May define **logical partitions**
-- MAGIC     * [WindowSpec.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.partitionBy.html#pyspark.sql.WindowSpec.partitionBy)
-- MAGIC 1. May define **frame boundary**
-- MAGIC     * [Window.rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.rangeBetween.html#pyspark.sql.Window.rangeBetween) and [WindowSpec.rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.rangeBetween.html#pyspark.sql.WindowSpec.rangeBetween)
-- MAGIC     * [Window.rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.rowsBetween.html#pyspark.sql.Window.rowsBetween) and [WindowSpec.rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.rowsBetween.html#pyspark.sql.WindowSpec.rowsBetween)
-- MAGIC 1. May define **ordering**
-- MAGIC     * [Window.orderBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.orderBy.html#pyspark.sql.Window.orderBy)
-- MAGIC     * [WindowSpec.orderBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.orderBy.html#pyspark.sql.WindowSpec.orderBy)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Window Factory Object

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Use [Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#window) factory to create a `WindowSpec`

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC import org.apache.spark.sql.expressions.Window
-- MAGIC val departmentById = Window
-- MAGIC  .partitionBy("department")
-- MAGIC  .orderBy("id")
-- MAGIC  .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql import Window
-- MAGIC departmentById = (Window
-- MAGIC  .partitionBy("department")
-- MAGIC  .orderBy("id")
-- MAGIC  .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### over Column Operator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. `over` column operator defines a windowing column (_analytic clause_)
-- MAGIC 1. Applies window function over a window

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC val overUnspecifiedFrame = sum('someColumn).over()
-- MAGIC val overRange = rank over departmentById

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Use with `DataFrame.withColumn` or `DataFrame.select` operators

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC // FIXME
-- MAGIC // numbers.withColumn("max", max("num") over dividedBy2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Visit [The Internals of Spark SQL](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-Column.html#over)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Finding Most Populated Cities Per Country](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html)
-- MAGIC 1. [Calculating percent rank](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Calculating-percent-rank.html)
-- MAGIC 1. [Calculating Gap Between Current And Highest Salaries Per Department](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Calculating-Gap-Between-Current-And-Highest-Salaries-Per-Department.html)
-- MAGIC 1. [Finding Longest Sequence](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Longest-Sequence.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Caching and Persistence
-- MAGIC 
-- MAGIC [Slides](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-dataset-caching-and-persistence.html#/home)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # The Internals of Structured Query Execution
-- MAGIC 
-- MAGIC [Slides](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-internals-of-structured-query-execution.html#/home)
