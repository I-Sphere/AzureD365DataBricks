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
  .option("header", "true")
  .csv("/mnt/datalake/model/dimension/ADAXGeneralJournalAccountEntry.csv")
