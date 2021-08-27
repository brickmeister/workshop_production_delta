# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Intro to Unified Delta Processing
# MAGIC 
# MAGIC One of the hallmark innovations of Databricks and the Lakehouse vision is the establishing of a unified method for writing and reading data in a data lake. This unification of batch and streaming jobs has been called the post-lambda architecture for data warehousing. The flexibility, simplicity, and scalability of the new delta lake architecture has been pivotal towards addressing big data needs and has been gifted to the Linux Foundation.
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/Multi-Hop%20Delta%20Lake.png?raw=true">
# MAGIC 
# MAGIC See below links for more documentation:
# MAGIC * [Beyond Lambda](https://databricks.com/discover/getting-started-with-delta-lake-tech-talks/beyond-lambda-introducing-delta-architecture)
# MAGIC * [Delta Lake Docs](https://docs.databricks.com/delta/index.html)

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

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup checkpoint directory
# MAGIC */
# MAGIC 
# MAGIC val checkpointDir : String = "/tmp/delta-stream_6";

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
# MAGIC val checkpointDir2 : String = "/tmp/delta-stream_7";

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
# MAGIC # Reading 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC """
# MAGIC Create a read stream from the compacted lending club table
# MAGIC """
# MAGIC 
# MAGIC df_lending_club_delta = spark.readStream\
# MAGIC                              .format("delta")\
# MAGIC                              .table("lending_club_stream_compact")
# MAGIC 
# MAGIC 
# MAGIC display(df_lending_club_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC #Watermarking
# MAGIC 
# MAGIC <img src='https://spark.apache.org/docs/latest/img/structured-streaming-watermark-update-mode.png' /img, width = 800>
# MAGIC 
# MAGIC It is important to note that the output mode must be set either to "append" or "update". Complete cannot be used in conjunction with Watermarking by design, because it requires all the data to be preserved for outputting the whole result table to sink.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(df_lending_club_delta.select("next_pymnt_d").na.drop().distinct())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import window
# MAGIC 
# MAGIC """
# MAGIC Do some real time aggregations with watermarking
# MAGIC """
# MAGIC 
# MAGIC display(df_lending_club_delta.withWatermark("next_pymnt_d", "1 month")\
# MAGIC                              .groupBy(
# MAGIC                                   window("next_pymnt_d", "10 minutes", "5 minutes"))\
# MAGIC                              .sum())
