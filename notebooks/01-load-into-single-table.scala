// Databricks notebook source
// MAGIC %md
// MAGIC # 01 - Load data into an Azure SQL heap, non-partitioned, non-indexed, table
// MAGIC 
// MAGIC In Azure SQL terminology an Heap is a table with no clustered index. In this samples we'll load data into a table that as no index (clustered or non-clustered) as is not partitioned. This is the simplest scenario possibile and allows parallel load of data.
// MAGIC 
// MAGIC Sample is using the new sql-spark-connector (https://github.com/microsoft/sql-spark-connector). To install manually import the .jar file (available in GitHub repo's releases) into the cluster.

// COMMAND ----------

// MAGIC %md
// MAGIC Define variables used thoughout the script. Azure Key Value has been used to securely store sensitive data. More info here: [Create an Azure Key Vault-backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope)

// COMMAND ----------

val scope = "key-vault-secrets"

val storageAccount = "dmstore2";
val storageKey = dbutils.secrets.get(scope, "dmstore2-2");

val server = dbutils.secrets.get(scope, "srv001").concat(".database.windows.net");
val database = dbutils.secrets.get(scope, "db001");
val user = dbutils.secrets.get(scope, "dbuser001");
val password = dbutils.secrets.get(scope, "dbpwd001");

// COMMAND ----------

// MAGIC %md
// MAGIC Configure Spark to access Azure Blob Store

// COMMAND ----------

spark.conf.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", storageKey);

// COMMAND ----------

// MAGIC %md
// MAGIC Load the Parquet file generated in `00-create-parquet-file` notebook that contains LINEITEM data partitioned by Year and Month

// COMMAND ----------

val li = spark.read.parquet("wasbs://tpch@dmstore2.blob.core.windows.net/10GB/parquet/lineitem")

// COMMAND ----------

// MAGIC %md Loaded data is split in 20 partitions

// COMMAND ----------

li.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC Show data distribution across partitions

// COMMAND ----------

display(li.groupBy($"L_PARTITION_KEY").count.orderBy($"L_PARTITION_KEY"))

// COMMAND ----------

// MAGIC %md
// MAGIC Show schema of loaded data

// COMMAND ----------

li.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure you create on your Azure SQL the following LINEITEM table:
// MAGIC ```sql
// MAGIC create table [dbo].[LINEITEM_LOADTEST]
// MAGIC (
// MAGIC 	[L_ORDERKEY] [int] not null,
// MAGIC 	[L_PARTKEY] [int] not null,
// MAGIC 	[L_SUPPKEY] [int] null,
// MAGIC 	[L_LINENUMBER] [int] not null,
// MAGIC 	[L_QUANTITY] [decimal](15, 2) null,
// MAGIC 	[L_EXTENDEDPRICE] [decimal](15, 2) null,
// MAGIC 	[L_DISCOUNT] [decimal](15, 2) null,
// MAGIC 	[L_TAX] [decimal](15, 2) null,
// MAGIC 	[L_RETURNFLAG] [char](1) null,
// MAGIC 	[L_LINESTATUS] [char](1) null,
// MAGIC 	[L_SHIPDATE] [date] null,
// MAGIC 	[L_COMMITDATE] [date] null,
// MAGIC 	[L_RECEIPTDATE] [date] null,
// MAGIC 	[L_SHIPINSTRUCT] [char](25) null,
// MAGIC 	[L_SHIPMODE] [char](10) null,
// MAGIC 	[L_COMMENT] [varchar](44) null,
// MAGIC 	[L_PARTITION_KEY] [int] not null
// MAGIC )
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC Load the table using Bulk Copy API. As table is empty and there are no indexes and we suppose we will not have any other activity running on the table at the same time, we can enable the option `tableLock` so that data can be loaded in parallel, even if the table is *not* partitioned. By default Databricks will have execute one bulk load task for each CPU available in the worker nodes. 

// COMMAND ----------

val table_name = "dbo.LINEITEM_TESTLOAD"
val url = s"jdbc:sqlserver://$server;databaseName=$database;"

li.write 
  .format("com.microsoft.sqlserver.jdbc.spark") 
  .mode("overwrite") 
  .option("truncate", "true") 
  .option("url", url) 
  .option("dbtable", table_name) 
  .option("user", user) 
  .option("password", password) 
  .option("applicationintent", "ReadWrite") 
  .option("reliabilityLevel", "BEST_EFFORT") 
  .option("tableLock", "true") 
  .option("batchsize", "100000") 
  .save()