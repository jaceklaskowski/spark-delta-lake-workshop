-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta SQL
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Welcome
-- MAGIC 
-- MAGIC This notebook is part of **Module 3: the Delta Lake 1.2 Tutorial** to teach you how to use and think like a [Delta Lake](https://delta.io/) pro! 😎
-- MAGIC 
-- MAGIC This and other notebooks are available in [jaceklaskowski/spark-delta-lake-workshop](https://github.com/jaceklaskowski/spark-delta-lake-workshop) repository on GitHub.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Learning Resources
-- MAGIC 
-- MAGIC The recommended learning resources (for reading and watching) to get better equipped for the module:
-- MAGIC 
-- MAGIC 1. [Apache Spark](https://spark.apache.org)
-- MAGIC 1. [Delta Lake](https://delta.io)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/)
-- MAGIC 1. [Ensuring Consistency with ACID Transactions with Delta Lake (Loan Risk Data)](https://pages.databricks.com/rs/094-YMS-629/images/01-Delta%20Lake%20Workshop%20-%20Delta%20Lake%20Primer.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Disclaimer
-- MAGIC 
-- MAGIC I've been a huge fan of SQL and environments like Databricks where you can use Apache Spark, Spark SQL, Structured Streaming and Delta Lake with little to no coding but data engineering.
-- MAGIC 
-- MAGIC This notebook is a testimony of this `ALTER`ation (a pun wholeheartedly intended).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Environment

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC println(io.delta.VERSION)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Yeah...I hear you. We've got [Delta Lake 1.2.1](https://github.com/delta-io/delta/releases/tag/v1.2.1) already and this looks odd.
-- MAGIC 
-- MAGIC Fear not since we're in the realm of Databricks and they got us covered with the features of 1.2.1 and beyond.

-- COMMAND ----------

SET spark.sql.sources.default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Agenda
-- MAGIC 
-- MAGIC * Delta Lake SQL
-- MAGIC * Time Travel
-- MAGIC * Transaction Log Fundamentals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # CREATE TABLE
-- MAGIC 
-- MAGIC [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html)

-- COMMAND ----------

DROP TABLE IF EXISTS delta101

-- COMMAND ----------

CREATE TABLE delta101
USING parquet
PARTITIONED BY (month)
COMMENT 'Delta 101'
AS VALUES (0L, 'zero', 1) t(id, name, month)

-- COMMAND ----------

DESC EXTENDED delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC The above SQL commands are standard and supported by none other than [Spark SQL](https://spark.apache.org/docs/latest/sql-ref.html) itself (and other SQL environments, too).
-- MAGIC 
-- MAGIC Let's use some Delta Lake-specific SQLs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # CONVERT TO DELTA
-- MAGIC 
-- MAGIC (delta-specific)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#convert-a-parquet-table-to-a-delta-table)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#convert-to-delta)

-- COMMAND ----------

CONVERT TO DELTA delta101
PARTITIONED BY (month INT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # DESCRIBE DETAIL
-- MAGIC 
-- MAGIC (delta-specific)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#-delta-detail)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#describe-detail)

-- COMMAND ----------

DESCRIBE DETAIL delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # DESCRIBE HISTORY
-- MAGIC 
-- MAGIC (delta-specific)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#-delta-history)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#describe-history)

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # DML Commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC There are some other Delta Lake-specific SQL commands, but for the purpose of the demo we'll switch our focus on a subset of the ANSI SQL statements called **Data Manipulation Language (DML)** that allow us set the stage for further exploration:
-- MAGIC 
-- MAGIC * `INSERT`
-- MAGIC * `UPDATE`
-- MAGIC * `DELETE`
-- MAGIC * `MERGE`
-- MAGIC 
-- MAGIC From [Wikipedia](https://en.wikipedia.org/wiki/Data_manipulation_language):
-- MAGIC 
-- MAGIC > A **data manipulation language (DML)** is a computer programming language used for adding (inserting), deleting, and modifying (updating) data in a database.
-- MAGIC >
-- MAGIC > A DML is often a sublanguage of a broader database language such as SQL, with the DML comprising some of the operators in the language.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Spark SQL comes with a [limited suppport](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L423-L434) for the above DML commands given their nature. `INSERT INTO` works just fine but the others are unsupported by default (and throw exceptions when executed).
-- MAGIC 
-- MAGIC Unless you use a DML-compatible data source like Delta Lake.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## INSERT
-- MAGIC 
-- MAGIC [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-into.html)

-- COMMAND ----------

INSERT INTO delta101
VALUES (1L, 'one', 1)

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Given how succint SQL can be, I'm surprised to find such a verbose `SELECT` statement to display the content of a table. I'd rather prefer `DISPLAY table` (that seems so tempting within Databricks since it does offer `.display()` and `display()` in Scala and Python).

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## UPDATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-update.html#update-a-table)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/commands/update/)

-- COMMAND ----------

UPDATE delta101
SET name = 'one_UPDATE' WHERE id = 1

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## DELETE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-update.html#delete-from-a-table)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/commands/delete/)

-- COMMAND ----------

DELETE FROM delta101
WHERE id > 0

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This is the moment when your phone starts ringing wildly and an idea that something wrong may have happened with the table develops in your mind. Let's revert the changes.

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC 
-- MAGIC ## RESTORE
-- MAGIC 
-- MAGIC (delta-specific)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#-restore-delta-table)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#restore)

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

RESTORE TABLE delta101
TO VERSION AS OF 2

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## MERGE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/commands/merge/)

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

MERGE INTO delta101 AS target
USING (
  VALUES
    (1, 'not important', 1),
    (2, 'merge', 2) updates(id, name, month))
ON target.id = updates.id
WHEN MATCHED THEN
  UPDATE SET
    name = concat(target.name, '_merge')
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Time Travel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC It's worth mentioning another feature of Delta Lake called **Time Travel** for a similar experience like `RESTORE` (alongside `MERGE`).
-- MAGIC 
-- MAGIC Delta Lake time travel allows you to query an older snapshot of a Delta table. Read more in the [Delta Lake documentation](https://docs.delta.io/latest/delta-batch.html#read-a-table).
-- MAGIC 
-- MAGIC Note that there is no support for `SELECT VERSION AS OF` in Delta Lake OSS (that is available in Databricks). So, no SQL in Delta Lake OSS for time travel except `RESTORE`.
-- MAGIC 
-- MAGIC Nothing is lost though. You should use Python, Scala or Java and use `versionAsOf` or `timestampAsOf` options in batch and streaming queries.

-- COMMAND ----------

desc history delta101

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val q = spark.read.option("versionAsOf", 3).table("delta101")
-- MAGIC display(q)

-- COMMAND ----------

-- Databricks-specific
SELECT * FROM delta101
VERSION AS OF 3

-- COMMAND ----------

SELECT * FROM delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Other Commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC There are other SQL commands supported by Delta Lake:
-- MAGIC 
-- MAGIC 1. `GENERATE`
-- MAGIC 1. `OPTIMIZE`
-- MAGIC 1. `VACUUM`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## OPTIMIZE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/optimizations-oss.html#-delta-optimize)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#optimize)

-- COMMAND ----------

OPTIMIZE delta101

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

OPTIMIZE delta101 WHERE month = 1

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## VACUUM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#-delta-vacuum)
-- MAGIC 1. [The Internals of Delta Lake](https://books.japila.pl/delta-lake-internals/sql/#VACUUM)

-- COMMAND ----------

VACUUM delta101 RETAIN 1 HOURS DRY RUN

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled=false

-- COMMAND ----------

VACUUM delta101 RETAIN 0.001 HOURS DRY RUN

-- COMMAND ----------

VACUUM delta101 RETAIN 0.001 HOURS

-- COMMAND ----------

DESCRIBE HISTORY delta101

-- COMMAND ----------

-- SparkException is expected after so time-agressive VACUUM
-- RESTORE delta101
-- TO VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # ALTER TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC There is a group of commands that allows changing the schema or properties of a table. Read more in [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table Constraints
-- MAGIC 
-- MAGIC Among the `ALTER TABLE`s supported by Spark SQL, there are two extra in Delta Lake:
-- MAGIC 
-- MAGIC 1. `ALTER TABLE ADD CONSTRAINT`
-- MAGIC 1. `ALTER TABLE DROP CONSTRAINT`
-- MAGIC 
-- MAGIC They are required for [Table Constraints](https://books.japila.pl/delta-lake-internals/constraints/) feature in Delta Lake.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Delta Lake 1.2
-- MAGIC 
-- MAGIC There are more features in Delta Lake based on `ALTER TABLE`s that were added just in [1.2](https://github.com/delta-io/delta/releases/tag/v1.2.0):
-- MAGIC 
-- MAGIC 1. [Column Mapping](https://books.japila.pl/delta-lake-internals/column-mapping/)
-- MAGIC 1. [Generated Columns](https://books.japila.pl/delta-lake-internals/generated-columns/)
-- MAGIC 
-- MAGIC One more seems to be coming in the next release, i.e. [IDENTITY columns](https://twitter.com/jaceklaskowski/status/1536439908914561025).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # THE END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Any questions?
