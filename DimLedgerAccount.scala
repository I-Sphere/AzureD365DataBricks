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

display(hierarchyDF)

// COMMAND ----------

hierarchyDF
  .write
  .mode(SaveMode.Overwrite)
  .option("header", "true")
  .csv("/mnt/datalake/model/dimension/LedgerAccount.csv")

// COMMAND ----------

hierarchyDF
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .option("path", "/mnt/datalake/model/database/LedgerAccount/LedgerAccount")

display(hierarchyDF)

// COMMAND ----------

hierarchyDF
  .write
  .format("delta")
  .mode(SaveMode.Append)
  .option("path", "/mnt/datalake/model/database/LedgerAccount/LedgerAccount")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS D365.LedgerAccount;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS D365.LedgerAccount
// MAGIC   (
// MAGIC     NAME string,
// MAGIC     MAINACCOUNT string,
// MAGIC     MAINACCOUNTID_L1 string,
// MAGIC     MAINACCOUNTID_L2 string,
// MAGIC     MAINACCOUNTID_L3 string,
// MAGIC     MAINACCOUNTID_L4 string,
// MAGIC     MAINACCOUNTID_L5 string,
// MAGIC     MAINACCOUNTID_L6 string,
// MAGIC     MAINACCOUNTID_L7 string,
// MAGIC     MAINACCOUNTID_L8 string
// MAGIC   )
// MAGIC   USING delta
// MAGIC   LOCATION "/mnt/datalake/model/database/LedgerAccount/LedgerAccount"
// MAGIC ;
// MAGIC   
// MAGIC select count(1) from D365.LedgerAccount;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED D365.LedgerAccount
