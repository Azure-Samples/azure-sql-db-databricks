// Databricks notebook source
// MAGIC %md
// MAGIC # 01 - Load data into an Azure SQL heap, non-partitioned, non-indexed, table
// MAGIC 
// MAGIC In Azure SQL terminology an Heap is a table with no clustered index. In this samples we'll load data into a table that as no index (clustered or non-clustered) as is not partitioned. This is the simplest scenario possibile and allows parallel load of data.
// MAGIC 
// MAGIC Sample is using both the new sql-spark-connector (https://github.com/microsoft/sql-spark-connector), and the previous one (https://github.com/Azure/azure-sqldb-spark). To install the _new connector_ manually import the .jar file (available in GitHub repo's releases) into the cluster. To install the previous one, just import the library right from Databricks portal using the "com.microsoft.azure:azure-sqldb-spark:1.0.2" coordinates.

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
val table = "dbo.LINEITEM_LOADTEST"


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
// MAGIC All columns are shown as nullable, even if they were originally set to NOT NULL, so we will need to keep this in mind later.

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure you create on your Azure SQL the following LINEITEM table:
// MAGIC ```sql
// MAGIC create table [dbo].[LINEITEM_LOADTEST]
// MAGIC (
// MAGIC 	[L_ORDERKEY] [int] not null,
// MAGIC 	[L_PARTKEY] [int] not null,
// MAGIC 	[L_SUPPKEY] [int] not null,
// MAGIC 	[L_LINENUMBER] [int] not null,
// MAGIC 	[L_QUANTITY] [decimal](15, 2) not null,
// MAGIC 	[L_EXTENDEDPRICE] [decimal](15, 2) not null,
// MAGIC 	[L_DISCOUNT] [decimal](15, 2) not null,
// MAGIC 	[L_TAX] [decimal](15, 2) not null,
// MAGIC 	[L_RETURNFLAG] [char](1) not null,
// MAGIC 	[L_LINESTATUS] [char](1) not null,
// MAGIC 	[L_SHIPDATE] [date] not null,
// MAGIC 	[L_COMMITDATE] [date] not null,
// MAGIC 	[L_RECEIPTDATE] [date] not null,
// MAGIC 	[L_SHIPINSTRUCT] [char](25) not null,
// MAGIC 	[L_SHIPMODE] [char](10) not null,
// MAGIC 	[L_COMMENT] [varchar](44) not null,
// MAGIC 	[L_PARTITION_KEY] [int] not null
// MAGIC ) 
// MAGIC ```

// COMMAND ----------

display(li.filter($"L_PARTITION_KEY" === 199202))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Using the new connector

// COMMAND ----------

// MAGIC %md
// MAGIC Schema needs to be defined explicitly as new connector is very sensitive to nullability, as per the following issue [Nullable column mismatch between Spark DataFrame & SQL Table Error](
// MAGIC https://github.com/microsoft/sql-spark-connector/issues/5), so we need to explicity create the schema and apply it to the loaded data

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(
    StructField("L_ORDERKEY", IntegerType, false) ::
    StructField("L_PARTKEY", IntegerType, false) ::
    StructField("L_SUPPKEY", IntegerType, false) ::  
    StructField("L_LINENUMBER", IntegerType, false) ::
    StructField("L_QUANTITY", DecimalType(15,2), false) ::
    StructField("L_EXTENDEDPRICE", DecimalType(15,2), false) ::
    StructField("L_DISCOUNT", DecimalType(15,2), false) ::
    StructField("L_TAX", DecimalType(15,2), false) ::
    StructField("L_RETURNFLAG", StringType, false) ::
    StructField("L_LINESTATUS", StringType, false) ::
    StructField("L_SHIPDATE", DateType, false) ::
    StructField("L_COMMITDATE", DateType, false) ::
    StructField("L_RECEIPTDATE", DateType, false) ::
    StructField("L_SHIPINSTRUCT", StringType, false) ::  
    StructField("L_SHIPMODE", StringType, false) ::  
    StructField("L_COMMENT", StringType, false) ::  
    StructField("L_PARTITION_KEY", IntegerType, false) ::  
    Nil)
    
  val li2 = spark.createDataFrame(li.rdd, schema)

// COMMAND ----------

val url = s"jdbc:sqlserver://$server;databaseName=$database;"

li2.write 
  .format("com.microsoft.sqlserver.jdbc.spark") 
  .mode("overwrite")   
  .option("truncate", "true") 
  .option("url", url) 
  .option("dbtable", table) 
  .option("user", user) 
  .option("password", password) 
  .option("reliabilityLevel", "BEST_EFFORT") 
  .option("tableLock", "true") 
  .option("batchsize", "100000") 
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC # Using the old connector:

// COMMAND ----------

// MAGIC %md
// MAGIC This connector is more permissive about schema so we can just use the schema coming from Parquet file

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url" -> server,
  "databaseName" -> database,
  "dbTable" -> table,
  "user" -> user,
  "password" -> password,
  "bulkCopyBatchSize" -> "100000",
  "bulkCopyTableLock" -> "true",  
  "bulkCopyTimeout" -> "600" //seconds  
))

li.bulkCopyToSqlDB(config)