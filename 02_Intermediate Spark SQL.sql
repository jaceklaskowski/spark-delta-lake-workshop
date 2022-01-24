-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Intermediate Spark SQL
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Welcome
-- MAGIC 
-- MAGIC This is the second module (out of 4) to teach you how to use and think like a Spark SQL and Delta Lake pro.
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
-- MAGIC ### Module 2. Intermediate Spark SQL (this notebook)
-- MAGIC 
-- MAGIC * Part 1
-- MAGIC   * Aggregations and Joins
-- MAGIC * Part 2
-- MAGIC   * [Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
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
-- MAGIC * [Apache Spark](https://spark.apache.org)
-- MAGIC * [Delta Lake](https://delta.io)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Aggregations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Aggregate functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC * **Aggregate functions** accept a group of records as input
-- MAGIC     * Unlike regular functions that act on a single record
-- MAGIC * Available among [standard functions](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)
-- MAGIC     * `import org.apache.spark.sql.functions._`
-- MAGIC * Usual suspects: `avg`, `collect_list`, `count`, `min`, `mean`, `sum`
-- MAGIC * You can create custom user-defined aggregate functions (UDAFs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## agg Operator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. `agg` applies an aggregate function to one or more records in a `Dataset`
-- MAGIC 
-- MAGIC     ```
-- MAGIC     val ds = spark.range(10)
-- MAGIC     ds.agg(sum('id) as "sum")
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Entire Dataset acts as a single group
-- MAGIC     * `groupBy` used to define groups
-- MAGIC 1. Creates a `DataFrame`
-- MAGIC     * ...hence considered untyped due to `Row` inside
-- MAGIC     * Typed variant available
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/Dataset/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Untyped groupBy Operator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. `groupBy` groups records in a `Dataset` per so-called _discriminator function_
-- MAGIC 
-- MAGIC     ```
-- MAGIC     val nums = spark.range(10)
-- MAGIC     nums.groupBy('id % 2 as "group").agg(sum('id) as "sum")
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Creates [RelationalGroupedDataset](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/RelationalGroupedDataset.html)
-- MAGIC     * Supports untyped, `Row`-based `agg`
-- MAGIC     * Shortcuts for the usual suspects, e.g. `avg`, `count`, `max`
-- MAGIC     * Supports `pivot`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Typed groupByKey Operator
-- MAGIC 
-- MAGIC 1. `groupByKey` is `groupBy` with typed interface
-- MAGIC 
-- MAGIC     ```
-- MAGIC     ds.groupByKey(_ % 2).reduceGroups(_ + _).show
-- MAGIC     ```
-- MAGIC 
-- MAGIC     ```
-- MAGIC     // compare to untyped query
-- MAGIC     nums.groupBy('id % 2 as "group").agg(sum('id) as "sum")
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Creates [KeyValueGroupedDataset](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/KeyValueGroupedDataset.html)
-- MAGIC     * Supports typed `agg`
-- MAGIC     * Shortcuts for the usual suspects, e.g. `reduceGroups`, `mapValues`, `mapGroups`, `flatMapGroups`, `cogroup`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## User-Defined Untyped Aggregate Functions (UDAFs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [UserDefinedAggregateFunction](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html)
-- MAGIC     * The base class for implementing user-defined aggregate functions
-- MAGIC     * **Deprecated since 3.0.0!**
-- MAGIC 1. [Aggregator](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/Aggregator.html)
-- MAGIC     * Registered as a UDF via the `functions.udaf(agg)` method
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/expressions/Aggregator/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Using Scala for Python or SQL Workloads

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [SparkSessionExtensionsProvider](https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/SparkSessionExtensionsProvider.html)
-- MAGIC 1. [SparkSessionExtensions](https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/SparkSessionExtensions.html)
-- MAGIC     * `injectFunction` written in Scala for Python or SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC We're going to use [Azure Databricks](https://azure.microsoft.com/en-us/services/databricks/#overview) and the [Databricks datasets](https://docs.databricks.com/data/databricks-datasets.html) to learn Spark SQL.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /databricks-datasets

-- COMMAND ----------

SELECT * FROM json.`dbfs:/databricks-datasets/nyctaxi/sample/json`

-- COMMAND ----------

DESCRIBE (SELECT * FROM json.`dbfs:/databricks-datasets/nyctaxi/sample/json`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Joins

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Join Operators

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. You can join two `Datasets` using `join` operators
-- MAGIC     1. join for untyped Row-based joins
-- MAGIC     1. Type-preserving joinWith
-- MAGIC     1. crossJoin for explicit cartesian joins
-- MAGIC 
-- MAGIC 1. Join conditions in `join` or `filter` / `where` operators
-- MAGIC 
-- MAGIC     ```
-- MAGIC     // equivalent to left("id") === right("id")
-- MAGIC     left.join(right, "id")
-- MAGIC     left.join(right, "id", "anti") // explicit join type
-- MAGIC     left.join(right).filter(left("id") === right("id"))
-- MAGIC     left.join(right).where(left("id") === right("id"))
-- MAGIC     ```
-- MAGIC 
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/Dataset/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Join Condition
-- MAGIC 
-- MAGIC 1. Join conditions in `join` or `filter` / `where` operators
-- MAGIC 
-- MAGIC     ````
-- MAGIC     // equivalent to left("id") === right("id")
-- MAGIC     left.join(right, "id")
-- MAGIC     // explicit join type
-- MAGIC     left.join(right, "id", "anti")
-- MAGIC     left.join(right).filter(left("id") === right("id"))
-- MAGIC     left.join(right).where(left("id") === right("id"))
-- MAGIC     // expr AND expr in multiple where's
-- MAGIC     left.join(right)
-- MAGIC       .where(left("id") === right("empid"))
-- MAGIC       .where(left("name") === right("empname"))
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Uses [Column](https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/Column.html) API
-- MAGIC 
-- MAGIC     ```
-- MAGIC     left("id") === right("empid")
-- MAGIC     left("id") === right("empid") AND left("name") === right("empname")
-- MAGIC     ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Join Types

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC SQL	| Names
-- MAGIC -----|-------
-- MAGIC CROSS | cross
-- MAGIC INNER | inner
-- MAGIC FULL OUTER | outer, full, fullouter
-- MAGIC LEFT ANTI | leftanti
-- MAGIC LEFT OUTER | leftouter, left
-- MAGIC LEFT SEMI | leftsemi
-- MAGIC RIGHT OUTER	| rightouter, right

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Join Clause (SQL)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. You can join two tables using regular SQL join operators
-- MAGIC 1. Use `SparkSession.sql` to execute SQL
-- MAGIC     ```
-- MAGIC     spark.sql("select * from t1, t2 where t1.id = t2.id")
-- MAGIC     ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Broadcast Join (Map-Side Join)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Use `broadcast` function to mark a `Dataset` to be broadcast
-- MAGIC     
-- MAGIC     ```
-- MAGIC     left.join(broadcast(right), "token")
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Use broadcast hints in SQL
-- MAGIC 
-- MAGIC     ```
-- MAGIC     SELECT /*+ BROADCAST (t1) */ * FROM t1, t2 WHERE t1.id = t2.id
-- MAGIC     BROADCAST, BROADCASTJOIN or MAPJOIN hints supported
-- MAGIC     ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Internals And Optimizations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Join Logical Operator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Join](https://books.japila.pl/spark-sql-internals/logical-operators/Join/)
-- MAGIC     * Binary logical operator with logical operators for the left and right side, a join type and an optional join expression

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### JoinSelection Execution Planning Strategy

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [JoinSelection](https://books.japila.pl/spark-sql-internals/execution-planning-strategies/JoinSelection/) is an execution planning strategy that SparkPlanner uses to plan `Join` logical operators
-- MAGIC 1. Supported joins (and their physical operators)
-- MAGIC     1. Broadcast Hash Join ([BroadcastHashJoinExec](https://books.japila.pl/spark-sql-internals/physical-operators/BroadcastHashJoinExec/))
-- MAGIC     1. Shuffled Hash Join ([ShuffledHashJoinExec](https://books.japila.pl/spark-sql-internals/physical-operators/ShuffledHashJoinExec))
-- MAGIC     1. Sort Merge Join ([SortMergeJoinExec](https://books.japila.pl/spark-sql-internals/physical-operators/SortMergeJoinExec))
-- MAGIC     1. Broadcast Nested Loop Join (BroadcastNestedLoopJoinExec)
-- MAGIC     1. Cartesian Join (CartesianProductExec)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Join Optimization — Bucketing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. **Bucketing** is an optimization technique in Spark SQL that uses buckets and bucketing columns to determine data partitioning
-- MAGIC 1. Optimize performance of a join query by reducing shuffles (aka exchanges)
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/bucketing/)
-- MAGIC 1. My talk [Bucketing in Spark SQL 2 3](https://www.youtube.com/watch?v=dv7IIYuQOXI) on Spark+AI Summit 2018

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Join Optimization — Join Reordering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. **Join Reordering** is an optimization of a logical query plan that the Spark Optimizer uses for joins
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/logical-optimizations/ReorderJoin)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Join Optimization — Cost-Based Join Reordering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. **Cost-based Join Reordering** is an optimization of a logical query plan that the Spark Optimizer uses for joins in cost-based optimization
-- MAGIC 1. Switch to [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/logical-optimizations/CostBasedJoinReorder)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Adaptive Query Execution (AQE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Adaptive Query Execution (AQE)](https://books.japila.pl/spark-sql-internals/adaptive-query-execution/)
