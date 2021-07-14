// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

var dimDF = spark
  .read
  .option("header", "true")
  .parquet("/mnt/datalake/MainAccountStaging/MainAccountStaging*.parquet")


// COMMAND ----------

dimDF = dimDF
  .select(
    $"MAINACCOUNTID",
    $"NAME"
  )
  .dropDuplicates()
  .orderBy("MAINACCOUNTID")
  .withColumn("MAINACCOUNT", reverse(reverse($"MAINACCOUNTID").cast("int").cast("string")))

display(
  dimDF
)

// COMMAND ----------

var hierarchyDF = dimDF
  .withColumn("MAINACCOUNTID_L1", substring($"MAINACCOUNT", 1, 1))
  .withColumn("MAINACCOUNTID_L2", substring($"MAINACCOUNT", 1, 2))
  .withColumn("MAINACCOUNTID_L3", substring($"MAINACCOUNT", 1, 3))
  .withColumn("MAINACCOUNTID_L4", substring($"MAINACCOUNT", 1, 4))
  .withColumn("MAINACCOUNTID_L5", substring($"MAINACCOUNT", 1, 5))
  .withColumn("MAINACCOUNTID_L6", substring($"MAINACCOUNT", 1, 6))
  .withColumn("MAINACCOUNTID_L7", substring($"MAINACCOUNT", 1, 7))
  .withColumn("MAINACCOUNTID_L8", substring($"MAINACCOUNT", 1, 8))
  .drop("MAINACCOUNTID")
  .na.drop(Seq("MAINACCOUNT"))
  .orderBy("MAINACCOUNT")

// COMMAND ----------

hierarchyDF
  .write
  .option("header", "true")
  .csv("/mnt/datalake/model/dimension/LedgerAccount.csv")
