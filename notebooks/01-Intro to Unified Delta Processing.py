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
# MAGIC # Setup some databases

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Generate a random UUID for a database to ensure users don't clash with each other
# MAGIC """
# MAGIC
# MAGIC import uuid
# MAGIC
# MAGIC # create a database name
# MAGIC db_name = str(uuid.uuid4()).replace("-", "_")
# MAGIC
# MAGIC # retrieve the name
# MAGIC print(db_name)

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create a database with random UUID
# MAGIC """
# MAGIC
# MAGIC spark.sql(f"USE CATALOG hive_metastore")
# MAGIC spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
# MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
# MAGIC spark.sql(f"USE DATABASE {db_name}")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Remove checkpoint directories and files
# MAGIC """
# MAGIC
# MAGIC dbutils.fs.rm(f"/tmp/{db_name}", True)

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

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Setup a data set to create gzipped json files
# MAGIC """
# MAGIC
# MAGIC # get the schema from the parquet files
# MAGIC file_schema = (spark.read
# MAGIC                     .format("parquet")
# MAGIC                     .option("inferSchema", True)
# MAGIC                     .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
# MAGIC                     .limit(10)
# MAGIC                     .schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Spark Structured Streaming
# MAGIC
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/structuredStreaming.png?raw=true">

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import to_timestamp, col
# MAGIC
# MAGIC """
# MAGIC Read lending club stream
# MAGIC """
# MAGIC
# MAGIC # get the schema from the parquet files
# MAGIC dfLendingClub = (spark.readStream
# MAGIC                       .format("parquet")
# MAGIC                       .schema(file_schema)
# MAGIC                       .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
# MAGIC                       .withColumn("earliest_cr_line", to_timestamp(col("earliest_cr_line"), "MMM-yyyy"))
# MAGIC                       .withColumn("last_pymnt_d", to_timestamp(col("last_pymnt_d"), "MMM-yyyy"))
# MAGIC                       .withColumn("next_pymnt_d", to_timestamp(col("next_pymnt_d"), "MMM-yyyy"))
# MAGIC                       .withColumn("issue_d", to_timestamp(col("issue_d"), "MMM-yyyy")))
# MAGIC
# MAGIC # See the dataframe
# MAGIC display(dfLendingClub)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta With Checkpointing
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/checkpoint.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### No Auto Compacting

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Setup checkpoint directory
# MAGIC """
# MAGIC
# MAGIC checkpoint_dir = f"/tmp/{db_name}/bronze";

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Write the stream to delta lake
# MAGIC """
# MAGIC
# MAGIC (dfLendingClub.writeStream
# MAGIC               .format("delta")
# MAGIC               .option("checkpointLocation", checkpoint_dir)
# MAGIC               .option("path", f"/tmp/{db_name}/lending_club_stream_no_compact")
# MAGIC               .toTable("lending_club_stream_no_compact"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe table extended lending_club_stream_no_compact;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC display(dbutils.fs.ls(f"dbfs:/tmp/{db_name}/lending_club_stream_no_compact"))

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

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Setup checkpoint directory
# MAGIC """
# MAGIC
# MAGIC checkpointDir2 = f"/tmp/{db_name}/bronze_compact";

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Write the stream to delta lake
# MAGIC """
# MAGIC
# MAGIC (dfLendingClub.writeStream
# MAGIC               .format("delta")
# MAGIC               .option("checkpointLocation", checkpointDir2)
# MAGIC               .option("path", f"/tmp/{db_name}/lending_club_stream_compact")
# MAGIC               .table("lending_club_stream_compact"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe table extended lending_club_stream_compact;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC display(dbutils.fs.ls(f"dbfs:/tmp/{db_name}/lending_club_stream_compact"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

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
# MAGIC checkpoint_dir_1 : str = f"/tmp/{db_name}/silver"

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
# MAGIC          .option("path", f"/tmp/{db_name}/lending_club_stream_silver")\
# MAGIC          .table("lending_club_stream_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_SILVER;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC display(dbutils.fs.ls(f"dbfs:/tmp/{db_name}/lending_club_stream_silver"))

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
# MAGIC checkpoint_dir_2 : str = f"/tmp/{db_name}/silver_updates"

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
# MAGIC          .option("path", f"/tmp/{db_name}/lending_club_stream_silver_updates")\
# MAGIC          .table("lending_club_stream_silver_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_SILVER_UPDATES;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC display(dbutils.fs.ls(f"dbfs:/tmp/{db_name}/lending_club_stream_silver_updates"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Stream
# MAGIC
# MAGIC ### Watermarking
# MAGIC
# MAGIC <img src='https://spark.apache.org/docs/latest/img/structured-streaming-watermark-update-mode.png' >
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
# MAGIC checkpoint_dir_3 : str = f"/tmp/{db_name}/gold"

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
# MAGIC        .option("path", f"/tmp/{db_name}/lending_club_stream_gold")\
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
# MAGIC        .option("path", f"/tmp/{db_name}/lending_club_stream_gold")\
# MAGIC        .outputMode("complete")\
# MAGIC        .table("lending_club_stream_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED LENDING_CLUB_STREAM_GOLD;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC display(dbutils.fs.ls(f"dbfs:/tmp/{db_name}/lending_club_stream_gold"))
