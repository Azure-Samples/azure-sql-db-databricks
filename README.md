---
page_type: sample
languages:
- tsql
- sql
- scala
products:
- azure
- azure-databricks
- azure-blob-storage
- azure-key-vault
- azure-sql-database
description: "Fast Data Loading in Azure SQL DB using Azure Databricks"
urlFragment: "azure-sql-db-databricks"
---

# Fast Data Loading in Azure SQL DB using Azure Databricks

![License](https://img.shields.io/badge/license-MIT-green.svg)

<!-- 
Guidelines on README format: https://review.docs.microsoft.com/help/onboard/admin/samples/concepts/readme-template?branch=master

Guidance on onboarding samples to docs.microsoft.com/samples: https://review.docs.microsoft.com/help/onboard/admin/samples/process/onboarding?branch=master

Taxonomies for products and languages: https://review.docs.microsoft.com/new-hope/information-architecture/metadata/taxonomies?branch=master
-->

Azure Databricks and Azure SQL database can be used amazingly well together. This repo will help you to use the [latest connector](https://github.com/microsoft/sql-spark-connector) to load data into Azure SQL as fast as possible, using table partitions and column-store and all the known best-practices.

- [Partitioned Tables and Indexes](https://docs.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes)
- [Columnstore indexes: Overview](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview)
- [Columnstore indexes - Data loading guidance](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance)

## Samples

All the samples start from a partitioned Parquet file, created with data generated from the famous TPC-H benchmark. Free tools are available on TPC-H website to generate a dataset with the size you want:

http://www.tpc.org/tpch/

Once the Parquet file is available, 

- [Create Parquet File](./notebooks/00-create-parquet-file.ipynb)

the samples will guide you through the most common scenarios

- [Loading a non-partitioned table](./notebooks/01-load-into-single-table.ipynb)
- [Loading a partitioned table](./notebooks/01-load-into-partitioned-table.ipynb)
- [Loading a partitioned table via switch-in](./notebooks/03a-parallel-switch-in-load-into-partitioned-table-many.ipynb)

all samples will also show how to correctly load table if there are already indexes or if you want to use a column-store in Azure SQL.

### Bonus Samples: Reading data as fast as possible

Though this repo focuses on writing data as fast as possible into Azure SQL, I also understand that you may also want to know how to do the opposite: how the read data as fast as possible from Azure SQL into Apache Spark / Azure Databricks? For this reason in the folder `notebooks/read-from-azure-sql` you will find two samples that shows how to do exactly that:

- [Fast Reading from Azure SQ](./notebooks/read-from-azure-sql/fast-read.ipynb)
- [Pushing queries to Azure SQL](./notebooks/read-from-azurel-sql/push-down-queries.ipynb)

## Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the Microsoft Open Source Code of Conduct. For more information see the Code of Conduct FAQ or contact opencode@microsoft.com with any additional questions or comments.
