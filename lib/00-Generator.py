# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Generator Script to Feed Bronze Tables with Data

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

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.{to_timestamp};
# MAGIC 
# MAGIC /*
# MAGIC   Read lending club stream
# MAGIC */
# MAGIC 
# MAGIC // get the schema from the parquet files
# MAGIC val dfLendingClub : DataFrame = spark.read
# MAGIC                                      .format("parquet")
# MAGIC                                      .schema(file_schema)
# MAGIC                                      .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
# MAGIC                                      .withColumn("earliest_cr_line", to_timestamp($"earliest_cr_line", "MMM-yyyy")) // parse timestamp
# MAGIC                                      .withColumn("last_pymnt_d", to_timestamp($"last_pymnt_d", "MMM-yyyy"))         // parse timestamp
# MAGIC                                      .withColumn("next_pymnt_d", to_timestamp($"next_pymnt_d", "MMM-yyyy"))         // parse timestamp
# MAGIC                                      .withColumn("issue_d", to_timestamp($"issue_d", "MMM-yyyy"));                  //parse timestamp
# MAGIC 
# MAGIC 
# MAGIC // See the dataframe
# MAGIC display(dfLendingClub)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Write the lending club data into a bronze table using an infinite loop.
# MAGIC   This is a generator
# MAGIC */
# MAGIC 
# MAGIC while(true){
# MAGIC   dfLendingClub.write
# MAGIC                .format("delta")
# MAGIC                .mode("append")
# MAGIC                .saveAsTable("lending_club_stream_compact")
# MAGIC };
