# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Medallion Architecture
# MAGIC 
# MAGIC Fundamental to the lakehouse view of ETL/ELT is the usage of a multi-hop data architecture known as the medallion architecture.
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/Multi-Hop%20Delta%20Lake.png?raw=true"> 
# MAGIC 
# MAGIC See the following links below for more detail.
# MAGIC 
# MAGIC * [Medallion Architecture](https://databricks.com/solutions/data-pipelines)
# MAGIC * [Cost Savings with the Medallion Architecture](https://techcommunity.microsoft.com/t5/analytics-on-azure/how-to-reduce-infrastructure-costs-by-up-to-80-with-azure/ba-p/1820280)
# MAGIC * [Change Data Capture Streams with the Medallion Architecture](https://databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Drop streaming tables if they exist
# MAGIC --
# MAGIC 
# MAGIC drop table if exists lending_club_stream_silver;
# MAGIC drop table if exists lending_club_stream_silver_updates;
# MAGIC drop table if exists lending_club_stream_silver_updates;
# MAGIC drop table if exists lending_club_stream_gold

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Bronze Stream

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import DataFrame
# MAGIC 
# MAGIC """
# MAGIC Do some data deduplication on ingestion streams
# MAGIC """
# MAGIC 
# MAGIC df_bronze : DataFrame = spark.readStream\
# MAGIC                              .format("delta")\
# MAGIC                              .table("lending_club_stream_compact")
# MAGIC   
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Tuning with Optimize and Zorder
# MAGIC 
# MAGIC Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to co-locate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Legend:
# MAGIC * Gray dot = data point e.g., chessboard square coordinates
# MAGIC * Gray box = data file; in this example, we aim for files of 4 points each
# MAGIC * Yellow box = data file that’s read for the given query
# MAGIC * Green dot = data point that passes the query’s filter and answers the query
# MAGIC * Red dot = data point that’s read, but doesn’t satisfy the filter; “false positive”
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png)
# MAGIC 
# MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run a a select query to retrieve the average interest rate for a particular type of loan
# MAGIC --
# MAGIC 
# MAGIC SELECT avg(regexp_replace(int_rate, "\\%", "")) as AVG_PERCENT
# MAGIC FROM lending_club_stream_no_compact
# MAGIC WHERE TERM = ' 36 months';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Optimize and Z-order by length of loan and interest rate
# MAGIC --
# MAGIC 
# MAGIC OPTIMIZE LENDING_CLUB_STREAM_NO_COMPACT
# MAGIC ZORDER BY TERM, INT_RATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run the same select query at higher performance
# MAGIC --
# MAGIC 
# MAGIC SELECT avg(regexp_replace(int_rate, "\\%", "")) as AVG_PERCENT
# MAGIC FROM lending_club_stream_no_compact
# MAGIC WHERE TERM = ' 36 months';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Creating Silver Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Deduplicate data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Deduplicate Bronze level data
# MAGIC """
# MAGIC 
# MAGIC df_silver : DataFrame = df_bronze.distinct()
# MAGIC   
# MAGIC display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Silver Stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tune for Appends

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Tune the file sizes for silvers table to read
# MAGIC --
# MAGIC 
# MAGIC SET delta.tuneFileSizesForRewrites=false;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_1 : str = "/tmp/silver_check_1"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write deduplicated silver streams 
# MAGIC """
# MAGIC 
# MAGIC df_silver.writeStream\
# MAGIC          .format("delta")\
# MAGIC          .option("checkpointLocation", checkpoint_dir_1)\
# MAGIC          .table("lending_club_stream_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_SILVER;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/lending_club_stream_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tune for Upserts and Deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Tune the file sizes for silvers table to read
# MAGIC --
# MAGIC 
# MAGIC SET delta.tuneFileSizesForRewrites=true;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_2 : str = "/tmp/silver_check_2"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write deduplicated silver streams 
# MAGIC """
# MAGIC 
# MAGIC df_silver.writeStream\
# MAGIC          .format("delta")\
# MAGIC          .option("checkpointLocation", checkpoint_dir_2)\
# MAGIC          .table("lending_club_stream_silver_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_SILVER_UPDATES;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/lending_club_stream_silver_updates

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Creating Gold Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Aggregation

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import window
# MAGIC 
# MAGIC """
# MAGIC Do some real time aggregations with watermarking
# MAGIC """
# MAGIC 
# MAGIC df_gold : DataFrame = df_silver.withWatermark("next_pymnt_d", "1 month")\
# MAGIC                                .groupBy(
# MAGIC                                     window("next_pymnt_d", "10 minutes", "5 minutes"))\
# MAGIC                                .sum()
# MAGIC   
# MAGIC display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Gold Stream

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_3 : str = "/tmp/gold_check_1"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write aggregated gold stream.
# MAGIC This should trigger an error due to invalid column names
# MAGIC """
# MAGIC 
# MAGIC df_gold.writeStream\
# MAGIC        .format("delta")\
# MAGIC        .option("checkpointLocation", checkpoint_dir_3)\
# MAGIC        .outputMode("update")\
# MAGIC        .table("lending_club_stream_gold")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Fix column names for aggregation
# MAGIC """
# MAGIC 
# MAGIC new_columns = [column.replace("(","_").replace(")", "") for column in df_gold.columns]
# MAGIC 
# MAGIC df_gold.toDF(*new_columns)\
# MAGIC        .writeStream\
# MAGIC        .format("delta")\
# MAGIC        .option("checkpointLocation", checkpoint_dir_3)\
# MAGIC        .outputMode("complete")\
# MAGIC        .table("lending_club_stream_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_GOLD;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/lending_club_stream_gold
