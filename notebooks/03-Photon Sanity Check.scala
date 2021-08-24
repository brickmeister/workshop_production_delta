// Databricks notebook source
// MAGIC %md
// MAGIC ### Photon Debugging 

// COMMAND ----------

// DBTITLE 1,Enable Photon Advice Text 
// MAGIC %sql
// MAGIC set spark.databricks.adviceGenerator.acceleratedWithPhoton.enabled = true;

// COMMAND ----------

// DBTITLE 1,Run Explain on Photon-Enabled Cluster
sc.range(0, 100).toDF.write.mode(SaveMode.Overwrite).parquet("/tmp/photon/test.parquet")
spark.read.parquet("/tmp/photon/test.parquet").createOrReplaceTempView("photon_test_table")
spark.sql("EXPLAIN SELECT COUNT(*), SUM(value) FROM photon_test_table").collect().foreach(println)

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.range(0, 100).toDF.write.mode(SaveMode.Overwrite).parquet("/tmp/photon/test.parquet")
// MAGIC spark.read.parquet("/tmp/photon/test.parquet").createOrReplaceTempView("photon_test_table")
// MAGIC spark.sql("SELECT COUNT(*), SUM(value) FROM photon_test_table").collect().foreach(println)

// COMMAND ----------

// DBTITLE 1,Run Explain when Photon is Disabled
// MAGIC %md
// MAGIC 
// MAGIC <img src='https://raw.githubusercontent.com/brickmeister/workshop_production_delta/main/img/PhotonDisabledQueryPlan.png' /img> 
