-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Many Small Files Problem
-- MAGIC 
-- MAGIC by Jacek Laskowski (jacek@japila.pl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Motivation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Tworzenie ogromnej ilości plików na HDFS’ie w momencie zapisu danych
-- MAGIC 1. Wiele zapytań kończy się mergowaniem szeregu różnych tymczasowych widoków z wykorzystaniem `UNION ALL`, ludzie nie są świadomi tego, że w takich sytuacjach Spark będzie po prostu zapisywał każde view „osobno”, np.:
-- MAGIC     1. Mając w zapytaniu shuffle’a, korzystając z defaultowych ustawień spark.sql.shuffle.partitions (200) i łącząc w 4 widoki, Spark stworzy nawet 800 plików na HDFSie!
-- MAGIC 1. Jest to szczególnie problematyczne kiedy jest potrzeba odczytać dane z więcej niż jednej partycji – zazwyczaj trwa to kilka(naście) minut a Spark ma do przeprocesowania 20-30 tysięcy tasków z wykorzystaniem np. 40 core’ów.
-- MAGIC 1. Byłoby fajnie opisać jak ogarnąć że coś jest nie halo w widoku SQL (na końcu, w zapisie do tabeli jest widoczne ile plików i ile rekordów Spark zapisał)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Introduction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 1. [Apache spark small file problem, simple to advanced solutions](https://www.linkedin.com/pulse/apache-spark-small-file-problem-simple-advanced-solutions-garg/) (an article on LinkedIn)
