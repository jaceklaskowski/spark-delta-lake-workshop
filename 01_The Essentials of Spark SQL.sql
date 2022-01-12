-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # The Essentials of Spark SQL
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Welcome
-- MAGIC 
-- MAGIC This is the first module (out of 4) to teach you how to use and think like a Spark SQL and Delta Lake pro.
-- MAGIC 
-- MAGIC 1 module takes 1,5h (2 x 45 mins)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Schedule
-- MAGIC 
-- MAGIC ### Module 1. The Essentials of Spark SQL (this notebook)
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Databricks Platform
-- MAGIC   * Loading and Saving Datasets
-- MAGIC * Part 2
-- MAGIC   * Basic DataFrame Transformations
-- MAGIC   * Web UI
-- MAGIC   
-- MAGIC ### Module 2. Intermediate Spark SQL
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Aggregations and Joins
-- MAGIC * Part 2
-- MAGIC   * Data Sources
-- MAGIC   * Loading Datasets from Cloud Storage
-- MAGIC 
-- MAGIC ### Module 3. Advanced Spark SQL
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
-- MAGIC 1. [What is Azure Databricks?](https://docs.microsoft.com/en-us/azure/databricks/scenarios/what-is-azure-databricks)
-- MAGIC 1. [What is Databricks Data Science & Engineering?](https://docs.microsoft.com/en-us/azure/databricks/scenarios/what-is-azure-databricks-ws)
-- MAGIC 1. [Databricks Data Science & Engineering concepts](https://docs.microsoft.com/en-us/azure/databricks/getting-started/concepts)
-- MAGIC 1. [Azure Databricks architecture overview](https://docs.microsoft.com/en-us/azure/databricks/getting-started/overview)
-- MAGIC 1. [Data Science & Engineering workspace](https://docs.microsoft.com/en-us/azure/databricks/workspace/)
-- MAGIC 1. [Workspace assets](https://docs.microsoft.com/en-us/azure/databricks/workspace/workspace-assets)
-- MAGIC 1. [Databricks Runtime](https://docs.microsoft.com/en-us/azure/databricks/runtime/dbr)
-- MAGIC 1. [Manage notebooks](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage)
-- MAGIC 1. [Use notebooks](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-use)
-- MAGIC 1. [Introduction to Apache Spark](https://docs.microsoft.com/en-us/azure/databricks/getting-started/spark/)
-- MAGIC 1. [Quickstart: Run a Spark job on Azure Databricks Workspace using the Azure portal](https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal)
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC * [Apache Spark](https://spark.apache.org)
-- MAGIC * [Delta Lake](https://delta.io)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Module 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Why Spark SQL
-- MAGIC 
-- MAGIC 1. Apache Spark's module for structured data processing
-- MAGIC     * Working with structured data (of different format and storage)
-- MAGIC     * Structured queries on Apache Spark (RDD)
-- MAGIC 1. Distributed Query Engine
-- MAGIC 1. SQL
-- MAGIC 1. General-purpose programming languages like Scala, Python, Java and R
-- MAGIC     1. DataFrame API
-- MAGIC     1. Dataset API
-- MAGIC 1. Many built-in data sources
-- MAGIC     1. Columnar file formats (Parquet, ORC)
-- MAGIC     1. Text file formats (JSON, CSV, Avro)
-- MAGIC     1. Apache Hive
-- MAGIC     1. Apache Kafka
-- MAGIC     1. JDBC
-- MAGIC 1. Pluggable DataSource API
-- MAGIC 1. Advanced query optimizations
-- MAGIC     1. Regardless of input data format or storage
-- MAGIC     1. Filter Pushdown
-- MAGIC     1. Column Pruning
-- MAGIC     1. Whole-Stage Java Code Generation (CodeGen)
-- MAGIC     1. Cost-Based Optimization (CBO)
-- MAGIC 1. web UI
-- MAGIC 1. Hive Metastore
-- MAGIC 1. [Dataflow](https://research.google/pubs/pub43864/)
-- MAGIC 
-- MAGIC We'll get to the above by examples during this session.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Databricks Platform
-- MAGIC 
-- MAGIC 1. [Databricks datasets](https://docs.databricks.com/data/databricks-datasets.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls('/databricks-datasets'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Loading and Saving Datasets
-- MAGIC 
-- MAGIC 1. [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
-- MAGIC 1. [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)

-- COMMAND ----------

SELECT * FROM text.`dbfs:/databricks-datasets/README.md`

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls dbfs:/databricks-datasets/nyctaxi/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.read.format("text").load("dbfs:/databricks-datasets/nyctaxi/readme_nyctaxi.txt").show(truncate = False)

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls dbfs:/databricks-datasets/nyctaxi/sample/

-- COMMAND ----------

SELECT * FROM text.`dbfs:/databricks-datasets/nyctaxi/sample/README.md`

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls dbfs:/databricks-datasets/nyctaxi/sample/json

-- COMMAND ----------

SELECT * FROM json.`dbfs:/databricks-datasets/nyctaxi/sample/json`

-- COMMAND ----------

DESCRIBE EXTENDED json.`dbfs:/databricks-datasets/nyctaxi/sample/json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Basic DataFrame Transformations
-- MAGIC 
-- MAGIC * [Column](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html)
-- MAGIC     * `*`
-- MAGIC     * `as`, `alias` or `name` for aliases
-- MAGIC     * `===` for equality (!)
-- MAGIC     * `desc`, `desc_nulls_first` and `desc_nulls_last` (and for asc)
-- MAGIC     * `getItem` to access items in arrays and maps
-- MAGIC     * `over` for windowed aggregates
-- MAGIC     * `cast` for casting to a custom data type
-- MAGIC     * `when` and `otherwise` for conditional values
-- MAGIC * [Dataset](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)
-- MAGIC     * `select`, `selectExpr`, `filter`, `where`, `withColumn`
-- MAGIC     * `createOrReplaceTempView` to register a temporary view
-- MAGIC     * `explain` to show the logical and execution plans
-- MAGIC     * `randomSplit` to split records to two Datasets randomly
-- MAGIC     * `as` to converting a `Row`-based DataFrame to a Dataset
-- MAGIC     * `flatMap` to "explode" records

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Standard Functions
-- MAGIC 
-- MAGIC **Standard functions** (_native functions_) are built-in functions that transform the values of one or more columns into new values
-- MAGIC 
-- MAGIC * Aggregate functions (avg, count, sum)
-- MAGIC * Collection functions (explode, from_json, array_*)
-- MAGIC * Date time functions (current_timestamp, to_date, window)
-- MAGIC * Math functions (conv, factorial, pow)
-- MAGIC * Non-aggregate functions (array, broadcast, expr, lit)
-- MAGIC * Sorting functions (asc, asc_nulls_first, asc_nulls_last)
-- MAGIC * String functions (concat_ws, trim, upper)
-- MAGIC * UDF functions (callUDF, udf)
-- MAGIC * Window functions (rank, row_number)
-- MAGIC 
-- MAGIC #### Collection functions
-- MAGIC 
-- MAGIC * Array Algebra (e.g. array_contains, array_distinct, array_except, array_intersect, flatten, arrays_zip, arrays_overlap)
-- MAGIC * Map Algebra (e.g. map_concat, map_from_entries, map_keys, map_values)
-- MAGIC * explode, explode_outer
-- MAGIC * posexplode, posexplode_outer
-- MAGIC 
-- MAGIC #### Date time functions
-- MAGIC 
-- MAGIC * unix_timestamp
-- MAGIC * to_timestamp
-- MAGIC * window
-- MAGIC * from_utc_timestamp, to_utc_timestamp, months_between

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercise
-- MAGIC 
-- MAGIC 1. For Scala devs: review [functions](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html) object
-- MAGIC 1. For SQL users: [SHOW FUNCTIONS](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-functions.html)
-- MAGIC 1. For Python devs: review [functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### User-Defined Functions
-- MAGIC 
-- MAGIC **DANGER**:
-- MAGIC Use the standard functions whenever possible before reverting to custom UDFs. UDFs are a blackbox for Spark Optimizer and does not even try to optimize them.
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC * User-Defined Functions extend the "vocabulary" of Spark SQL
-- MAGIC * Use `udf` function to define a user-defined function (in Scala)
-- MAGIC * Use UDFs as standard functions
-- MAGIC     * `withColumn`, `select`, `filter`, etc.
-- MAGIC     * Also `callUDF` function
-- MAGIC * Use `spark.udf.register` to register a Scala function as a user-defined function to use in SQL

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val myUpperFn = (input: String) => input.toUpperCase
-- MAGIC spark.udf.register("myUpper", myUpperFn)

-- COMMAND ----------

SELECT myUpper(name) FROM VALUES ("hello"), ("world") AS t(name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercise
-- MAGIC 
-- MAGIC As a Python dev, define an UDF using [pyspark.sql.functions.udf](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.udf.html).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Web UI
