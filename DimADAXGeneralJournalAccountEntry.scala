// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

var dimDF = spark
  .read
  .option("header", "true")
  .parquet("/mnt/datalake/ADAXGeneralJournalAccountEntryStaging/ADAXGeneralJournalAccountEntryStaging*.parquet")

// COMMAND ----------

dimDF = dimDF
          .select(
            "ACCOUNTDISPLAYVALUE",
            "JOURNALNUMBER",
            "DOCUMENTNUMBER",
            "ACCOUNTINGDATE",
            "TRANSACTIONCURRENCYCODE",
            "TRANSACTIONCURRENCYCREDITAMOUNT",
            "TRANSACTIONCURRENCYDEBITAMOUNT",
            "DESCRIPTION",
            "POSTINGTYPE"
          )
          .withColumn("MAINACCOUNT", reverse(reverse($"ACCOUNTDISPLAYVALUE").cast("int").cast("string")))

display(dimDF)

// COMMAND ----------

dimDF
  .write
  .mode(SaveMode.Overwrite)
  .option("header", "true")
  .csv("/mnt/datalake/model/dimension/ADAXGeneralJournalAccountEntry.csv")

// COMMAND ----------

dimDF
  .write
  .format("delta")
  //.mode(SaveMode.Overwrite)
  .mode(SaveMode.Append)
  .option("path", "/mnt/datalake/model/database/ADAXGeneralJournalAccountEntry/ADAXGeneralJournalAccountEntry")
  //.saveAsTable("D365.ADAXGeneralJournalAccountEntry")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from D365.ADAXGeneralJournalAccountEntry

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED D365.ADAXGeneralJournalAccountEntry

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE D365.ADAXGeneralJournalAccountEntry

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS D365.ADAXGeneralJournalAccountEntry;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS D365.ADAXGeneralJournalAccountEntry
// MAGIC   (
// MAGIC     ACCOUNTDISPLAYVALUE string,
// MAGIC     JOURNALNUMBER string,
// MAGIC     DOCUMENTNUMBER string,
// MAGIC     ACCOUNTINGDATE timestamp,
// MAGIC     TRANSACTIONCURRENCYCODE string,
// MAGIC     TRANSACTIONCURRENCYCREDITAMOUNT decimal(12,6),
// MAGIC     TRANSACTIONCURRENCYDEBITAMOUNT decimal(12,6),
// MAGIC     DESCRIPTION string,
// MAGIC     POSTINGTYPE bigint,
// MAGIC     MAINACCOUNT string
// MAGIC   )
// MAGIC   USING delta
// MAGIC   LOCATION "/mnt/datalake/model/database/ADAXGeneralJournalAccountEntry/ADAXGeneralJournalAccountEntry"
// MAGIC   ;
// MAGIC   
// MAGIC select count(1) from D365.ADAXGeneralJournalAccountEntry;

// COMMAND ----------

dimDF
  .write
  .format("delta")
  .mode(SaveMode.Append)
  .option("path", "/mnt/datalake/model/database/ADAXGeneralJournalAccountEntry/ADAXGeneralJournalAccountEntry")
  //.saveAsTable("D365.ADAXGeneralJournalAccountEntry")

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE D365.ADAXGeneralJournalAccountEntry;
// MAGIC 
// MAGIC select count(1) from D365.ADAXGeneralJournalAccountEntry;
// MAGIC --order by JOURNALNUMBER, ACCOUNTDISPLAYVALUE, ACCOUNTINGDATE;
