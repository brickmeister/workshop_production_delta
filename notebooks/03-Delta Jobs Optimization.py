# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Quantifying Delta Performance and Debugging Jobs
# MAGIC 
# MAGIC Optimizing delta performance from a jobs and cluster perspective is a critical part of Production Delta Lake Performance. Steps to tease apart delta lake performance will often use a variety of methods listed in the table below.
# MAGIC 
# MAGIC |Feature|Use|Link|
# MAGIC |-------|---|----|
# MAGIC |Jobs| Production level automated runs of ETL/ML Pipelines|https://docs.databricks.com/jobs.html|
# MAGIC |Cluster Tuning | Controlling the knobs associated with eeking out maximal performance|https://docs.databricks.com/clusters/configure.html|
# MAGIC |DAG Tuning | Controlling the stages associated with a Spark Job and a Query Execution Plan | https://databricks.com/session/understanding-query-plans-and-spark-uis|
# MAGIC |Logs | Spark Driver and Cluster Logs that provide execution and autoscaling details | https://docs.databricks.com/spark/latest/rdd-streaming/debugging-streaming-applications.html|
# MAGIC 
# MAGIC Useful blogs to understand how Delta Lake performance tuning works are listed below.
# MAGIC * [Best Practices](https://docs.databricks.com/delta/best-practices.html)
# MAGIC * [Adaptive Query Execution](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Jobs UI
# MAGIC 
# MAGIC Jobs results are stored for up to 60 days. You can export job results to an external storage for longer term auditing. Looking at these job runs allows historical analyis of different pipelines.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/jobs%20UI.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Alerts
# MAGIC 
# MAGIC Setup alerts for delta job success and failure. Currently these jobs utilize an emailing alert that can be sent on job start, success, and failure.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/alerts.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retries
# MAGIC 
# MAGIC Setup retries to restart delta jobs when they fail. This is used to ensure there is a degree of fault tolerance associated with mission critical pipelines.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/retries.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Timeouts
# MAGIC When certain jobs hang, measure the expected time a job will take and set a timeout about 1.5x that time to reduce wasted resources.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/timeouts.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Concurrent Runs
# MAGIC 
# MAGIC Setup concurrent runs to allow multiple versions of a job to run simultaneously. This is useful when jobs contain different parameters to produce different flavors of a job that utilize the same template.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/concurrent%20runs.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schedule
# MAGIC 
# MAGIC Schedule jobs to ensure they meet SLAs.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/schedule.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Cluster Performance
# MAGIC 
# MAGIC Cluster performance tuning is an important step when quantifying delta performance. The distributed nature of Delta and Spark allow great horizontal scaling by adding more nodes to meet performance SLAs. Generally speaking, leverage autoscaling on Spark clusters to reduce costs and tackle peak throughput workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Live Metrics
# MAGIC 
# MAGIC Databricks cluster performance can be observed in the Ganglia UI which runs live on the cluster.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/ganglia%20ui.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Metrics
# MAGIC 
# MAGIC Databricks takes a snapshot of the live Ganglia Metrics UI every 15 mintues
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/historical%20ganglia%20metrics.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark UI
# MAGIC 
# MAGIC Databricks exposes the Spark UI which will provide a large amount of usable statistics to measure the performance of your jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Structured Streaming Performance
# MAGIC 
# MAGIC When measuring structured streaming performance for delta use cases, you can look at the observed average throughputs of the different streams running on the cluster and determine if they meet your SLAs.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/structured%20streaming%20stats.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark DAG
# MAGIC 
# MAGIC Every job in Spark consists of a series of spark tasks which form a directed acyclic graph (DAG). Examining these DAGs can help identify bottleneck stages to determine where more performance can be extracted. Notice the IO stats below the DAG to determine if portions of the job are spent on ingress/egress of data, or there is a lot of garbage collection going on.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/spark_dag.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## File cache
# MAGIC 
# MAGIC Databricks leverages filesystem caches to reduce ingress/egress from cloud object storage. The more the cache is leveraged, the less network I/O is needed and the faster the job will run. When you have multiple workloads that leverage the same table, running those workloads on the same cluster can reduce processing time since the underlying parquet files will not be constantly retrieved from object storage.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/storage%20cache%20stats.png?raw=true' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Logs
# MAGIC 
# MAGIC Databricks logs the driver output as well as cluster resizing events. These can be correlated with particular event times to determine what may have been a blocker or bottleneck in delta performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driver Logs
# MAGIC 
# MAGIC Looking for errors in the driver logs to determine if a job has failed due to an error.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/driver%20logs.png?raw=true' /img> 

# COMMAND ----------

# DBTITLE 1,Driver Logs - Photon is Enabled
# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/brickmeister/workshop_production_delta/main/img/DriverLog_AcceleratedwithPhotonEnabled.png' /img> 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Event Log
# MAGIC 
# MAGIC Look at the cluster event logs to determine if there were any autoscaling issues. Look specifically for node acquisition issues (due to cloud limits) or lack of autoscaling for an increasing workload size.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/event%20logs.png?raw=true' /img>
