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
# MAGIC ## Imports Libraries
# MAGIC It's necessary to import the `dlt` Python module to use the associated methods.
# MAGIC
# MAGIC Here, we also import `pyspark.sql.functions`.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
testsetest

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Set up Configurations

# COMMAND ----------

"""
Set up configuration 
"""

inputData1 =  'GradeA' #spark.conf.get("advancingdlt.pipeline.entityName1") 
inputData2 =  'GradeB' #spark.conf.get("advancingdlt.pipeline.entityName2") 

# COMMAND ----------

"""
Setup a data set to read schema
"""

@dlt.create_table(name = "lending_club_schema",
                  comment = "Small table to get lending club schema")

def data_raw_lending_club_schema():
  ## get the schema from the parquet files
  return (spark.read.format("parquet")
               .option("inferSchema", True)
               .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
               .limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Python Syntax
# MAGIC
# MAGIC DLT tables, views, and their associated settings are configured using [decorators](https://www.python.org/dev/peps/pep-0318/#current-syntax).
# MAGIC
# MAGIC If you're unfamiliar with Python decorators, just note that they are functions or classes preceded with the `@` sign that interact with the next function present in a Python script.
# MAGIC
# MAGIC The `@dlt.table` decorator is the basic method for turning a Python function into a Delta Live Table.

# COMMAND ----------

"""
Ingestion Data
"""

from pyspark.sql.functions import to_timestamp, col

@dlt.create_table(name="lending_club_raw",
                  comment = "Raw Lending Club Data")

def data_raw_lending_club_raw():
  return(spark.readStream.format("parquet")              
               .schema(dlt.read("lending_club_schema").schema)
              .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
              .withColumn("earliest_cr_line", to_timestamp(col("earliest_cr_line"), "MMM-yyyy"))
              .withColumn("last_pymnt_d", to_timestamp(col("last_pymnt_d"), "MMM-yyyy"))
              .withColumn("next_pymnt_d", to_timestamp(col("next_pymnt_d"), "MMM-yyyy"))
              .withColumn("issue_d", to_timestamp(col("issue_d"), "MMM-yyyy")))

# COMMAND ----------

"""
State table for valid grades
"""

@dlt.create_table(name="lending_club_cleaned",
                  comment = "Lending Club Grade state")

def data_raw_lending_club_cleaned():
  return(dlt.read_stream("lending_club_raw").distinct())

# COMMAND ----------

"""
State table for valid grades
"""

@dlt.create_table(name="lending_club_grades",
                  comment = "Lending Club Grade state")

def data_raw_lending_club_grades():
  return(dlt.read("lending_club_cleaned")
            .select("grade")
            .distinct())

# COMMAND ----------

"""
State table for valid payment dates
"""

@dlt.create_table(name="lending_club_payment_dates",
                  comment = "Lending Club Payment Dates")

def data_raw_lending_club_paymnt_dates():
  return(dlt.read("lending_club_cleaned")
            .select("next_pymnt_d")
            .distinct())

# COMMAND ----------

# DBTITLE 1,Ingest raw data - Bronze Layer
"""
Creating raw delta live tables
"""


@dlt.create_table(name=f"{inputData1}",
  comment="Raw batch 1 dataset ingested from /databricks-datasets - Grade A."
)

def data_raw_GradeA():          
  return (dlt.read_stream("lending_club_cleaned").where("grade=='A'"))

@dlt.create_table(name=f"{inputData2}",
  comment="Raw batch 2 dataset ingested from /databricks-datasets - Grade B."
)

def data_raw_GradeB():          
  return (dlt.read_stream("lending_club_cleaned").where("grade=='B'"))

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

# DBTITLE 0,Set up Quality Check
"""
Setting up expectations for quality check
"""

@dlt.table(name=f"Expected_{inputData1}",
  comment="Grade A data cleaned and prepared for analysis."
)
@dlt.expect_or_drop("valid loan_amnt","loan_amnt>1500")

def data_prepared():
  return (
    dlt.read_stream(f"{inputData1}")
  )

@dlt.table(name=f"Expected_{inputData2}",
  comment="Grade B data cleaned and prepared for analysis."
)
@dlt.expect_or_drop("valid funded_amnt","funded_amnt>2000")

def data_prepared():
  return (
    dlt.read_stream(f"{inputData2}")
  )

# COMMAND ----------

# DBTITLE 1,Extract Top Loanees 
"""
Do some data transformation and preprocessing
"""

@dlt.table(name=f"TopLoanees_{inputData1}",
  comment="A table containing the top loanees with grade A."
)
def top_loanees_MI():
  return (
    dlt.read(f"Expected_{inputData1}")
      .filter(expr("addr_state == 'MI'"))
      .sort(desc("loan_amnt"))
      .limit(10)
  )

@dlt.table(name=f"TopLoanees_{inputData2}",
  comment="A table containing the top lonees with grade B."
)
def top_loanees_CA():
  return (
    dlt.read(f"Expected_{inputData2}")
      .filter(expr("addr_state == 'CA'"))
      .sort(desc("loan_amnt"))
      .limit(10)
  )

# COMMAND ----------

# DBTITLE 1,Unify Top loanees of Grade A and Grade B
"""
Unify cleaned and preprocessed delta live tables
"""

@dlt.table(name=f"all_Top_lonees",
  comment="A table containing all the top lonees with both grade A and B."
)

def all_top_lonees():
    return (dlt.read(f"TopLoanees_{inputData1}")
        .union(dlt.read(f"TopLoanees_{inputData2}"))
    )
