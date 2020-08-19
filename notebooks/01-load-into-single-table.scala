// Databricks notebook source
// MAGIC %md
// MAGIC # 01 - Load data into an Azure SQL heap, non-partitioned, non-indexed, table
// MAGIC 
// MAGIC In Azure SQL terminoligy an Heap is a table with no clustered index. In this samples we'll load data into a table that as no index (clustered or non-clustered) as is not partitioned. This is the simplest scenario possibile and allows parallel load of data.

// COMMAND ----------

// MAGIC %md
// MAGIC Define variables used thoughout the script. Azure Key Value has been used to securely store sensitive data. More info here: [Create an Azure Key Vault-backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope)

// COMMAND ----------

val scope = "key-vault-secrets"

val storageAccount = "dmstore2";
val storageKey = dbutils.secrets.get(scope, "dmstore2-2");

val url = dbutils.secrets.get(scope, "srv001").concat(".database.windows.net");
val databaseName = dbutils.secrets.get(scope, "db001");
val user = dbutils.secrets.get(scope, "dbuser001");
val password = dbutils.secrets.get(scope, "dbpwd001");

// COMMAND ----------

// MAGIC %md
// MAGIC Configure Spark to access Azure Blob Store

// COMMAND ----------

spark.conf.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", storageKey);

// COMMAND ----------

