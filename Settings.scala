// Databricks notebook source
dbutils.fs.unmount(
  mountPoint = "/mnt/datalake"
)

// COMMAND ----------

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "c2ca71ae-6cdc-4717-93b5-c9c4cea095e9",
  "dfs.adls.oauth2.credential" -> ".nR4Lt2uO2rB1~6ZAL.Wilte_3D~79r~xl",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/2b7449c0-8f5e-4472-b3c8-cb0aa17c3dd2/oauth2/token"
)

dbutils.fs.mount(
  source = "adl://ispheredatalake.azuredatalakestore.net",
  mountPoint = "/mnt/datalake",
  extraConfigs = configs
)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS D365
