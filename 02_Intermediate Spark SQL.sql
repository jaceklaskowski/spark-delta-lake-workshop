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
-- MAGIC * [The Internals Online Books](https://books.japila.pl/)

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
-- MAGIC 
-- MAGIC     ```scala
-- MAGIC     import org.apache.spark.sql.functions._
-- MAGIC     ```
-- MAGIC 
-- MAGIC     ```python
-- MAGIC     from pyspark.sql import functions as F
-- MAGIC     ```
-- MAGIC 
-- MAGIC * Usual suspects: `avg`, `collect_list`, `count`, `min`, `mean`, `sum`
-- MAGIC * (Scala) You can create custom user-defined aggregate functions (UDAFs)
-- MAGIC     * `Aggregator[-IN, BUF, OUT]`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## DataFrame.agg Operator

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
-- MAGIC 
-- MAGIC     ```python
-- MAGIC     ds.groupBy().agg(F.sum("id") as "sum")
-- MAGIC     ```
-- MAGIC 
-- MAGIC 1. Use `groupBy` to define groups
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
-- MAGIC 1. `groupBy` groups records in a structured query (`Dataset`) per so-called _discriminator function_
-- MAGIC 
-- MAGIC     ```
-- MAGIC     val nums = spark.range(10)
-- MAGIC     nums.groupBy("id" % 2 as "group").agg(sum('id) as "sum")
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. Creates [RelationalGroupedDataset](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/RelationalGroupedDataset.html)
-- MAGIC     * Supports untyped, `Row`-based `agg`
-- MAGIC     * Shortcuts for the usual suspects, e.g. `avg`, `count`, `max`
-- MAGIC     * Supports `pivot`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## (Scala) Typed groupByKey Operator
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
-- MAGIC ## (Scala) User-Defined Untyped Aggregate Functions (UDAFs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [UserDefinedAggregateFunction](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html)
-- MAGIC     * The base class for implementing user-defined aggregate functions
-- MAGIC     * **Deprecated since 3.0.0!**
-- MAGIC 1. [Aggregator](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/Aggregator.html)
-- MAGIC     * Registered as a UDAF via the `functions.udaf(agg)` method
-- MAGIC 1. Use SparkSessionExtensions to enable custom UDAFs to any Spark application (incl. pySpark)
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
-- MAGIC ## Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC [Adding count to the source DataFrame](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/adding-count-to-the-source-dataframe.html)

-- COMMAND ----------

CREATE OR REPLACE TABLE AS VALUES
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2) labels(column0, column1, column2, label)

-- COMMAND ----------

SELECT * FROM labels

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val input = Seq(
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
-- MAGIC   ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2)).toDF("column0", "column1", "column2", "label")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC display(input)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC val counts = input.groupBy("column0", "column1", "column2").agg(count("label") as "count")
-- MAGIC display(counts)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC display(input.join(counts, Seq("column0", "column1", "column2"), "INNER"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Finding Ids of Rows with Word in Array Column](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html)
-- MAGIC     * Current time: 3pm
-- MAGIC     * Exercise time: 15'
-- MAGIC 1. [Using pivot for Cost Average and Collecting Values](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html)
-- MAGIC     * Current time: 3:45pm
-- MAGIC     * Exercise time: 15'
-- MAGIC 1. [Calculating aggregations](https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-value-agg.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercise 1

-- COMMAND ----------

-- Thanks Yegor!

SELECT
  w,
  ARRAY_SORT(COLLECT_SET(id)) as ids
FROM (
  SELECT 
  *,
  -- Is EXPLODE a LATERAL VIEW?
  EXPLODE(SPLIT(words,",")) AS w
  FROM (VALUES (1,"one,two,three","one"),
          (2,"four,one,five","six"),
          (3,"seven,nine,one,two","eight"),
          (4,"two,three,five","five"),
          (5,"six,five,one","seven")) AS t(id,words,word) 
)
WHERE w IN ('five','one','six','seven')
GROUP BY w
ORDER BY w

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC words = spark.createDataFrame(
-- MAGIC     [
-- MAGIC         ('1', 'one,two,three', 'one'),
-- MAGIC         ('2', 'four,one,five', 'six'),
-- MAGIC         ('3', 'seven,nine,one,two', 'eight'),
-- MAGIC         ('4', 'two,three,five','five'),
-- MAGIC         ('5', 'six,five,one','seven'),
-- MAGIC     ], [
-- MAGIC         'id', 'words','word'
-- MAGIC     ]
-- MAGIC )
-- MAGIC display(words)

-- COMMAND ----------

-- Optionally, register the dataset as a table (for SQL queries)
CREATE OR REPLACE TABLE words AS
VALUES
  (1,"one,two,three","one"),
  (2,"four,one,five","six"),
  (3,"seven,nine,one,two","eight"),
  (4,"two,three,five","five"),
  (5,"six,five,one","seven") AS t(id,words,word)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC data = spark.table("words")
-- MAGIC from pyspark.sql import functions as F
-- MAGIC words = data.withColumn("w", F.explode(F.split("words", ",")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Thanks Kasia!
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC solution = data\
-- MAGIC     .withColumn("split_values", F.split("words", ","))\
-- MAGIC     .withColumn("word", F.explode("split_values"))\
-- MAGIC     .groupby("word")\
-- MAGIC     .agg(F.collect_set("id").alias("ids"))\
-- MAGIC     .join(data, on='word')\
-- MAGIC     .select(F.col("word").alias("w"), F.array_sort("ids").alias("ids"))\
-- MAGIC     .orderBy("w")
-- MAGIC display(solution)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Exercise 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Thanks Barbara!
-- MAGIC 
-- MAGIC df = sql(
-- MAGIC f"""
-- MAGIC SELECT *
-- MAGIC FROM VALUES 
-- MAGIC     ("0", "A", "223", "201603", "PORT"),
-- MAGIC     ("0", "A", "22", "201602", "PORT"),
-- MAGIC     ("0", "A", "422", "201601", "DOCK"),
-- MAGIC     ("1", "B", "3213", "201602", "DOCK"),
-- MAGIC     ("1", "B", "3213", "201601", "PORT"),
-- MAGIC     ("2", "C", "2321", "201601", "DOCK")
-- MAGIC AS t1(id,type,cost,date,ship)
-- MAGIC """
-- MAGIC )
-- MAGIC 
-- MAGIC part1 = df.groupBy('id', 'type').pivot('date').agg(F.avg("cost")).orderBy('type')
-- MAGIC display(part1)
-- MAGIC 
-- MAGIC part2 = df.groupBy('id','type').pivot('date').agg(F.collect_set("ship")).orderBy('type')
-- MAGIC display(part2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Thanks Kasia!
-- MAGIC data = df
-- MAGIC solution = data.groupBy(["id","type"]).pivot("date").agg(F.avg("cost")).orderBy("id")
-- MAGIC # Watch out the types as you can face the following exception
-- MAGIC # "cost" is not a numeric column. Aggregation function can only be applied on a numeric column.
-- MAGIC # cost: string
-- MAGIC # solution = data.groupBy(["id","type"]).pivot("date").avg("cost").orderBy("id")
-- MAGIC display(solution)

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
-- MAGIC 1. You can join two structured queries (`DataFrame`s) using `join` operators
-- MAGIC     1. `join` for untyped Row-based joins
-- MAGIC     1. (Scala) Type-preserving `joinWith`
-- MAGIC     1. `crossJoin` for explicit cartesian joins
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

-- MAGIC %python
-- MAGIC 
-- MAGIC # FIXME Why does this join work (thought it'd require `crossJoin`?!)
-- MAGIC 
-- MAGIC left = spark.range(5)
-- MAGIC display(left.join(left))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val left = spark.range(5)
-- MAGIC display(left.join(left, left("id") === left("id")))

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC d1 = spark.range(5)
-- MAGIC d2 = spark.range(5)
-- MAGIC 
-- MAGIC display(d1.join(d2, "id"))
-- MAGIC 
-- MAGIC q.explain(extended = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # where is simply an alias to filter
-- MAGIC q = d1.join(d2).where(d1["id"] == d2["id"])
-- MAGIC display(q)

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
-- MAGIC ### Fun Facts
-- MAGIC 
-- MAGIC 1. Join names are case-insensitive
-- MAGIC 1. Names can be written down with `_` (underscores)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(d1.join(d2, "id", "i_n_NE_R"))
-- MAGIC display(d1.join(d2, "id", "iNNeR"))
-- MAGIC display(d1.join(d2, "id", "inner"))

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
-- MAGIC #### FIXME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Use `SqlBase.g4` to learn more
-- MAGIC 1. FIXME Demo different `joinCriteria` (`ON` and `USING`)

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
-- MAGIC     ```
-- MAGIC     
-- MAGIC 1. BROADCAST, BROADCASTJOIN or MAPJOIN hints supported
-- MAGIC 1. `spark.sql.autoBroadcastJoinThreshold` configuration property

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

-- MAGIC %python
-- MAGIC 
-- MAGIC q = d1.join(d2, "id")
-- MAGIC q.explain(extended = True)

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
-- MAGIC 1. [The official documentation of Spark SQL](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
-- MAGIC 1. [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/adaptive-query-execution/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### FIXME
-- MAGIC 
-- MAGIC 1. Work on demos for different AQE optimizations based on [The official documentation of Spark SQL](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Current time: 4:50pm
-- MAGIC 2. Break: 10'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Finding Most Populated Cities Per Country](https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC cities = spark\
-- MAGIC     .read\
-- MAGIC     .format("csv")\
-- MAGIC     .option("header", "true")\
-- MAGIC     .load("dbfs:/FileStore/shared_uploads/jacek@japila.pl/cities-2.csv")
-- MAGIC cities.createOrReplaceTempView("cities")

-- COMMAND ----------

SELECT * FROM cities

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Thanks Kuba
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC cities_with_pop_long = cities\
-- MAGIC     .withColumn("pop", F.translate("population", " ", "").cast("long"))
-- MAGIC biggest_cities_per_country = cities_with_pop_long\
-- MAGIC     .groupBy('country')\
-- MAGIC     .agg(F.max("pop").alias("max_population"))
-- MAGIC solution = biggest_cities_per_country\
-- MAGIC     .join(cities_with_pop_long, ['country'])\
-- MAGIC     .where(biggest_cities_per_country["max_population"] == cities_with_pop_long["pop"])\
-- MAGIC     .select('name', 'country', 'population')
-- MAGIC display(solution)

-- COMMAND ----------

-- Kuba's Solution
-- FIXME type of the population column
SELECT 
    orig.name,
    pars.country,
    pars.population
FROM(
    SELECT 
        country,
        FIRST(name) AS name,
        MAX(population) AS population
    FROM (
        SELECT * 
        FROM cities
        ORDER BY population DESC
    )
    GROUP BY country
) AS pars
LEFT JOIN cities AS orig
ON pars.country = orig.country AND pars.name = orig.name

-- COMMAND ----------

-- Yegor's solution
-- Mind it uses window aggregation
SELECT 
        name, 
        country, 
        population
    FROM (
        SELECT 
            *,
            MAX(CAST(REGEXP_REPLACE(population," ","") AS LONG)) OVER (PARTITION BY country) AS m
        FROM (VALUES ("Warsaw","Poland","1 764 615"),
                    ("Cracow","Poland","769 498"),
                    ("Paris","France","2 206 488"),
                    ("Villeneuve-Loubet","France","15 020"),
                    ("Pittsburgh PA","United States","302 407"),
 ("Chicago IL","United States","2 716 000"),
                    ("Milwaukee WI","United States","595 351"),
                    ("Vilnius","Lithuania","580 020"),
                    ("Stockholm","Sweden","972 647"),
                    ("Goteborg","Sweden","580 020")) AS t(name,country,population))
    WHERE CAST(REGEXP_REPLACE(population," ","") AS INT) = m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Questions
-- MAGIC 
-- MAGIC 1. Using Spark predicate pushdown in Spark SQL queries?  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC q = cities.where("name = 'Warsaw'")
-- MAGIC display(q)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC cities.write.parquet("cities_pq")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC cities_pq = spark.read.parquet("dbfs:/cities_pq")
-- MAGIC q = cities_pq.where("name = 'Warsaw'")
-- MAGIC display(q)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Current time: 6:05pm
