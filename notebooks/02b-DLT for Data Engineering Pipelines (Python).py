# Databricks notebook source
# MAGIC %md # Delta Live Tables - Lending Club (Python)
# MAGIC 
# MAGIC This notebook uses Python to declare Delta Live Tables. Note that this syntax is not intended for interactive execution in a notebook.
# MAGIC 
# MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#python).
# MAGIC 
# MAGIC [Delta Live Tables API guide](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-api-guide.html#get-pipeline-details)
# MAGIC 
# MAGIC This notebook provides an example Delta Live Tables pipeline to:
# MAGIC 
# MAGIC - Read raw data into a table.
# MAGIC - Read records from the raw data table and use a Delta Live Tables query and expectations to create a new table with cleaned and prepared data.
# MAGIC - Perform an analysis on the prepared data with a Delta Live Tables query.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Cloud storage paths for ingest, delta lake and checkpoints
# MAGIC It's necessary to import the `dlt` Python module to use the associated methods.
# MAGIC 
# MAGIC Here, we also import `pyspark.sql.functions`.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

username = "mojgan.mazouchi@databricks.com"
userPrefix = username.split("@")[0].replace(".", "")
basePath = "/mnt/morgan-demo/" + username + "/autoloaderDemo"
landingZoneLocation = basePath + "/landingZone"
schemaLocation = basePath + "/schemaStore"
bronzeTableLocation = basePath + "/datastore/bronzeTbl" 
bronzeCheckPointLocation = basePath + "/datastore/bronzeCheckpoint"
spark.conf.set("c.bronzeTablePath", "dbfs:" + bronzeTableLocation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Python Syntax
# MAGIC 
# MAGIC DLT tables, views, and their associated settings are configured using [decorators](https://www.python.org/dev/peps/pep-0318/#current-syntax).
# MAGIC 
# MAGIC If you're unfamiliar with Python decorators, just note that they are functions or classes preceded with the `@` sign that interact with the next function present in a Python script.
# MAGIC 
# MAGIC The `@dlt.table` decorator is the basic method for turning a Python function into a Delta Live Table.
# MAGIC 
# MAGIC The function must return a PySpark or Koalas DataFrame. Here we're using [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) to incrementally ingest files from object storage.

# COMMAND ----------

# DBTITLE 1,Load sample data and create test batches
sourceDF = (spark.read.format("csv") \
                  .option("inferSchema", "true") \
                  .option("header", "true") \
                  .load("dbfs:/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv")
               )


batch1 = sourceDF.filter(sourceDF.grade=="A")
batch2 =sourceDF.filter(sourceDF.grade=="B")
batch3 = sourceDF.filter(sourceDF.grade=="C")

# COMMAND ----------

# DBTITLE 1,Write batch-1 Data to S3
batch1.write.parquet(landingZoneLocation)

# COMMAND ----------

inputData1 =  spark.conf.get("advancingdlt.pipeline.entityName1") #'LendingclubGradeA'
inputData2 =  spark.conf.get("advancingdlt.pipeline.entityName2") #'LendingclubGradeB'
dataPath = landingZoneLocation
print(dataPath)

# COMMAND ----------

# DBTITLE 1,Setup Expectations
# MAGIC %sql
# MAGIC 
# MAGIC drop table config.sources;
# MAGIC 
# MAGIC create database if not exists Config;
# MAGIC 
# MAGIC create table if not exists config.sources
# MAGIC (TableName string,
# MAGIC ReadOptions string,
# MAGIC Expectations string
# MAGIC );
# MAGIC 
# MAGIC insert into config.sources
# MAGIC select 'LendingclubGradeA', '{"Header":"True", "Sep":",","inferSchema":"True"}','{"Valid_loan_status":"loan_status IS NOT NULL" , "Valid_loan_amnt" : "loan_amnt>1500"}';
# MAGIC 
# MAGIC insert into config.sources
# MAGIC select 'LendingclubGradeB', '{"Header":"True", "Sep":",","inferSchema":"True"}','{"Valid_loan_status":"loan_status IS NOT NULL" , "Valid_loan_amnt" : "loan_amnt>1500"}';
# MAGIC 
# MAGIC select * from config.sources

# COMMAND ----------

# DBTITLE 0,Imports
import json

conf = spark.table("config.sources").filter(f"TableName = '{inputData1}'").first()
dfconf = json.loads(conf.ReadOptions)
expectconf = json.loads(conf.Expectations)

print(f"data frame options: {dfconf}")
print(f"expectations: {expectconf}")

# COMMAND ----------

df = spark.read.options(**dfconf).parquet(dataPath)

display(df)

# COMMAND ----------

# DBTITLE 1,Ingest raw data - Bronze Layer
import dlt

@dlt.create_table(name=f"DLT_{inputData1}",
  comment="Raw dataset ingested from /databricks-datasets."
)

def data_raw():          
  return (spark.readStream.options(**dfconf)\
            .format("cloudFiles")\
            .option("cloudFiles.format", "parquet") \
            .option("cloudFiles.schemaLocation", schemaLocation)\
            .option("cloudFiles.validateOptions", "false") \
            .option("cloudFiles.region", "us-west-2") \
            .option("cloudFiles.includeExistingFiles", "true") \
            #.option("cloudFiles.useNotifications", "true")
            .option("cloudFiles.allowOverwrites", "true")   
            .option("failOnUnknownFields", "true")\
            .schema(sourceDF.schema).load(dataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Control with Expectations
# MAGIC 
# MAGIC Data expectations are expressed as simple filters against a field in a table.
# MAGIC 
# MAGIC DLT currently supports three modes for expectations:
# MAGIC 
# MAGIC | mode | behavior |
# MAGIC | --- | --- |
# MAGIC | `@dlt.expect` | Record metrics for percentage of records that fulfill expectation <br> (**NOTE**: this metric is reported for all execution modes) |
# MAGIC | `@dlt.expect_or_fail` | Fail when expectation is not met |
# MAGIC | `@dlt.expect_or_drop` | Only process records that fulfill expectations |

# COMMAND ----------

# DBTITLE 1,Clean and prepare data - Quality Check Table 1
@dlt.table(name=f"DLT_prepared_{inputData1}",
  comment="Grade A data cleaned and prepared for analysis."
)
@dlt.expect_all(expectconf)

def data_prepared():
  return (
    dlt.read(f"DLT_{inputData1}").withColumnRenamed("addr_state", "state")\
          .select("loan_amnt", "funded_amnt", "term", "int_rate", "grade", "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "loan_status","state","total_pymnt")
  )

# COMMAND ----------

# DBTITLE 1,Write batch-2 data to S3
batch2.write.mode("append").parquet(dataPath)

# COMMAND ----------

@dlt.create_table(name=f"DLT_{inputData2}",
  comment="Grade B Raw dataset ingested from /databricks-datasets."
)

def data_raw():          
  return (spark.readStream.options(**dfconf)\
            .format("cloudFiles")\
            .option("cloudFiles.format", "parquet") \
            .option("cloudFiles.schemaLocation", schemaLocation)\
            .option("cloudFiles.validateOptions", "false") \
            .option("cloudFiles.region", "us-west-2") \
            .option("cloudFiles.includeExistingFiles", "true") \
            #.option("cloudFiles.useNotifications", "true")
            .option("cloudFiles.allowOverwrites", "true")   
            .option("failOnUnknownFields", "true")\
            .schema(sourceDF.schema).load(dataPath))

# COMMAND ----------

# DBTITLE 1,Clean and prepare data - Quality Check Table 2
@dlt.table(name=f"DLT_prepared_{inputData2}",
  comment="Grade B data cleaned and prepared for analysis."
)
@dlt.expect_all(expectconf)

def data_prepared2():
  return (
    dlt.read(f"DLT_{inputData2}").withColumnRenamed("addr_state", "state")\
          .select("loan_amnt", "funded_amnt", "term", "int_rate", "grade", "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "loan_status","state","total_pymnt")
  )

# COMMAND ----------

# DBTITLE 1,Top loanees in MI with Grade A
@dlt.table(name=f"Top_{inputData1}_GradeA",
  comment="A table containing the top loanees with grade A."
)
def top_loanees_MI():
  return (
    dlt.read(f"DLT_prepared_{inputData1}")
      .filter(expr("state == 'MI'"))
      .sort(desc("loan_amnt"))
      .select("loan_amnt", "funded_amnt", "term", "int_rate", "grade", "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "loan_status","state","total_pymnt" )
      .limit(10)
  )

# COMMAND ----------

# DBTITLE 1,Top loanees in CA with Grade B
@dlt.table(name=f"Top_{inputData2}_GradeB",
  comment="A table containing the top lonees with grade B."
)
def top_loanees_CA():
  return (
    dlt.read(f"DLT_prepared_{inputData2}")
      .filter(expr("state == 'CA'"))
      .sort(desc("loan_amnt"))
      .select("loan_amnt", "funded_amnt", "term", "int_rate", "grade", "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "loan_status","state","total_pymnt" )
      .limit(10)
  )

# COMMAND ----------

# DBTITLE 1,Unify Top loanees of Grade A and Grade B
@dlt.table(name=f"all_Top_lonees",
  comment="A table containing all the top lonees with both grade A and B."
)

def all_top_lonees():
    return (dlt.read(f"Top_{inputData1}_GradeA")
        .union(dlt.read(f"Top_{inputData2}_GradeB"))
        .select("loan_amnt", "funded_amnt", "term", "int_rate", "grade", "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "loan_status","state","total_pymnt" )
    )
