# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Compliance
# MAGIC 
# MAGIC When it comes to developing and validating a production level lakehouse, there are several critical features that are often required.
# MAGIC 
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/Delta%20Lake%20Primer.png?raw=true'>
# MAGIC 
# MAGIC |Feature| Use| Links|
# MAGIC |-------|----|------|
# MAGIC |Data Version History| Audit log for any changes made to a table | https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-describe-history.html#describe-history-delta-lake-on-databricks|
# MAGIC |Rollbacks | Restore Previous Versions of a Table | https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html#restore-delta-lake-on-databricks|
# MAGIC |Data Deletes | Delete row level data | https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-delete-from.html#delete-from-delta-lake-on-databricks|
# MAGIC 
# MAGIC See the following links below for some great blogs and documentation on our delta lake features for compliance.
# MAGIC 
# MAGIC * [GDPR & CCPA](https://docs.databricks.com/security/privacy/gdpr-delta.html)
# MAGIC * [Disaster Recovery](https://www.youtube.com/watch?v=z24dC4ZwL50)
# MAGIC * [Delta Updates](https://docs.databricks.com/delta/delta-update.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ACID Compliance
# MAGIC 
# MAGIC Delta Lake is an ACID transaction layer ontop of your existing data lake. This means that there is a transaction log that records changes made to tables. Accessing this 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table Version History
# MAGIC 
# MAGIC The table version history is recorded on a timestamp and version number basis for each change made to a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get the version history of our silver loans table
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE HISTORY lending_club_stream_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Rollback
# MAGIC 
# MAGIC Whenever corrupted data is written to a table, it is important to be rollback to a version of the table that does not contain the corrupted data. Delta Lake tables support rollback using the restore command.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Restore a table to a previous version
# MAGIC --
# MAGIC 
# MAGIC RESTORE LENDING_CLUB_STREAM_SILVER TO VERSION AS OF 98;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get the version history of our silver loans table after restore
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE HISTORY lending_club_stream_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Deletes
# MAGIC 
# MAGIC Delta lake enables per row deletes of data utilizing the transaction log. However, upon deleting data, a subsequent operation needs to be run to remove physical traces of deleted data. This process coupled with a grace period is often used to achieve GDPR & CCPA compliance on Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look for the data that will be marked for deletion
# MAGIC --
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM LENDING_CLUB_STREAM_SILVER 
# MAGIC WHERE ZIP_CODE = '130xx';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Delete a row of data
# MAGIC --
# MAGIC 
# MAGIC DELETE FROM LENDING_CLUB_STREAM_SILVER WHERE ZIP_CODE = '130xx';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look for the deleted data
# MAGIC --
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM LENDING_CLUB_STREAM_SILVER 
# MAGIC WHERE ZIP_CODE = '130xx';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Vacuum
# MAGIC 
# MAGIC Due to the copy on write nature of Delta Lake, deletes rows aren't deleted but instead marked for deletion. In order to remove physical traces of deleted data, we run a vacuum command with a retention time

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Remove any deleted data
# MAGIC --
# MAGIC 
# MAGIC VACUUM LENDING_CLUB_STREAM_SILVER RETAIN 168 HOURS;
