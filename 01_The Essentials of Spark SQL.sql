-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # The Essentials of Spark SQL
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This is the first module (out of 4) to teach you how to use and think like a Spark SQL and Delta Lake pro.
-- MAGIC 
-- MAGIC * [Apache Spark](https://spark.apache.org)
-- MAGIC * [Delta Lake](https://delta.io)
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
-- MAGIC 1. (optional) [Quickstart: Run a Spark job on Azure Databricks Workspace using the Azure portal](https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal)

-- COMMAND ----------


