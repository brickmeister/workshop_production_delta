# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Intro to Unified Delta Processing and the Medallion Architecture
# MAGIC 
# MAGIC One of the hallmark innovations of Databricks and the Lakehouse vision is the establishing of a unified method for writing and reading data in a data lake. This unification of batch and streaming jobs has been called the post-lambda architecture for data warehousing. The flexibility, simplicity, and scalability of the new delta lake architecture has been pivotal towards addressing big data needs and has been gifted to the Linux Foundation. Fundamental to the lakehouse view of ETL/ELT is the usage of a multi-hop data architecture known as the medallion architecture.
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/Multi-Hop%20Delta%20Lake.png?raw=true">
# MAGIC 
# MAGIC See below links for more documentation:
# MAGIC * [Beyond Lambda](https://databricks.com/discover/getting-started-with-delta-lake-tech-talks/beyond-lambda-introducing-delta-architecture)
# MAGIC * [Delta Lake Docs](https://docs.databricks.com/delta/index.html)
# MAGIC * [Medallion Architecture](https://databricks.com/solutions/data-pipelines)
# MAGIC * [Cost Savings with the Medallion Architecture](https://techcommunity.microsoft.com/t5/analytics-on-azure/how-to-reduce-infrastructure-costs-by-up-to-80-with-azure/ba-p/1820280)
# MAGIC * [Change Data Capture Streams with the Medallion Architecture](https://databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup a delta stream   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup a data set to create gzipped json files
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.rand;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC 
# MAGIC // get the schema from the parquet files
# MAGIC val file_schema : StructType = spark.read
# MAGIC                                 .format("parquet")
# MAGIC                                 .option("inferSchema", true)
# MAGIC                                 .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
# MAGIC                                 .limit(10)
# MAGIC                                 .schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark Structured Streaming                                                   
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/structuredStreaming.png?raw=true", width = 600> 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.{to_timestamp};
# MAGIC 
# MAGIC /*
# MAGIC   Read lending club stream
# MAGIC */
# MAGIC 
# MAGIC // get the schema from the parquet files
# MAGIC val dfLendingClub : DataFrame = spark.readStream
# MAGIC                                        .format("parquet")
# MAGIC                                        .schema(file_schema)
# MAGIC                                        .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
# MAGIC                                        .withColumn("earliest_cr_line", to_timestamp($"earliest_cr_line", "MMM-yyyy")) // parse timestamp
# MAGIC                                        .withColumn("last_pymnt_d", to_timestamp($"last_pymnt_d", "MMM-yyyy"))         // parse timestamp
# MAGIC                                        .withColumn("next_pymnt_d", to_timestamp($"next_pymnt_d", "MMM-yyyy"))         // parse timestamp
# MAGIC                                        .withColumn("issue_d", to_timestamp($"issue_d", "MMM-yyyy"));                  //parse timestamp
# MAGIC 
# MAGIC // See the dataframe
# MAGIC display(dfLendingClub);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta With Checkpointing
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/checkpoint.png?raw=true", width = 500> 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### No Auto Compacting

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Drop streaming tables if they exist
# MAGIC --
# MAGIC 
# MAGIC drop table if exists lending_club_stream_no_compact;
# MAGIC drop table if exists lending_club_stream_compact;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Turn on auto compacting for new tables
# MAGIC --
# MAGIC 
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup checkpoint directory
# MAGIC */
# MAGIC 
# MAGIC val checkpointDir : String = "/tmp/delta-stream_200000000";

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Write the stream to delta lake
# MAGIC */
# MAGIC 
# MAGIC dfLendingClub.writeStream
# MAGIC              .format("delta")
# MAGIC              .option("checkpointLocation", checkpointDir)
# MAGIC              .table("lending_club_stream_no_compact")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe table extended lending_club_stream_no_compact;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/lending_club_stream_no_compact

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Auto Compacting
# MAGIC 
# MAGIC Auto Compaction occurs after a write to a table has succeeded and runs synchronously on the cluster that has performed the write. This means that if you have code patterns where you make a write to Delta Lake, and then immediately call OPTIMIZE, you can remove the OPTIMIZE call if you enable Auto Compaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Turn on auto compacting for new tables
# MAGIC --
# MAGIC 
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup checkpoint directory
# MAGIC */
# MAGIC 
# MAGIC val checkpointDir2 : String = "/tmp/delta-stream_800000000";

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Write the stream to delta lake
# MAGIC */
# MAGIC 
# MAGIC dfLendingClub.writeStream
# MAGIC              .format("delta")
# MAGIC              .option("checkpointLocation", checkpointDir2)
# MAGIC              .table("lending_club_stream_compact")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe table extended lending_club_stream_compact;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/lending_club_stream_compact

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

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
# MAGIC ## Creating Bronze Tables

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
# MAGIC ### Tuning with Optimize and Zorder
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
# MAGIC ## Creating Silver Tables

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
# MAGIC ### Tune for Bulk Writes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Tune the file sizes for silvers table to read
# MAGIC --
# MAGIC 
# MAGIC SET spark.databricks.delta.tuneFileSizesForRewrites=false;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_1 : str = "/tmp/silver_check_100000000"

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
# MAGIC SET spark.databricks.delta.tuneFileSizesForRewrites=true;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_2 : str = "/tmp/silver_check_200000000"

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
# MAGIC ## Write Gold Stream
# MAGIC 
# MAGIC ### Watermarking
# MAGIC 
# MAGIC <img src='https://spark.apache.org/docs/latest/img/structured-streaming-watermark-update-mode.png' /img, width = 800>
# MAGIC 
# MAGIC It is important to note that the output mode must be set either to "append" or "update". Complete cannot be used in conjunction with Watermarking by design, because it requires all the data to be preserved for outputting the whole result table to sink.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowed Aggregation

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(df_silver.select("next_pymnt_d").na.drop().distinct())

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

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_3 : str = "/tmp/gold_check_100000000"

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
# MAGIC        .outputMode("complete")\
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
