### Databricks: Upsert to Azure SQL using PySpark
An Upsert is an RDBMS feature that allows a DML statement’s author to automatically either insert a row or if the row already exists, UPDATE that existing row instead.
From my experience building multiple Azure Data Platforms I have been able to develop reusable ELT functions that I can use from project to project, one being an Azure SQL upsert function.
Today I’m going to share with you have to how to create an Azure SQL Upsert function using PySpark. It can be reused across Databricks workflows with minimal effort and flexibility.

### Basic Upsert Logic
Two tables are created, one staging table and one target table
Data is loaded into the staging table
The tables are joined on lookup columns and/or a delta column to identify the matches
If the record in the staging table exists in the target table, the record is updated in the target table
If the record in the staging table does not exist in the target table, it is inserted into the target table
Azure SQL Upsert PySpark Function
Functionality
An input data frame is written to a staging table on Azure SQL
The function accepts a parameter for multiple Lookup columns and/or an optional Delta column to join the staging and target tables
If a delta column is passed to the function it will update the record in the target table only if the staging table record is newer than the target table record
The function will dynamically read the Dataframe columns to form part of the SQL Merge upsert and insert statements
Before writing code, it is critical to understand the Spark Azure SQL Database connector. The connector does not support preUpdate or postUpdate statements following writing to a table. For this reason, we need to write the Dataframe to the staging table and subsequently pass the valid SQL merge statements to the PyODBC connector to execute the upsert.

## Prerequisite
* Azure SQL Target and Staging tables to be created with the correct data types and indexes to  improve join performance
* Install Apache Spark connector for SQL Server and Azure SQL on your cluster: https://github.com/* microsoft/sql-spark-connector
* Run the following code on your cluster to install the PyODBC

```
%sh
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc
```

NOTE: When you restart your cluster or create a new one these settings will be lost and you will need to run this again. Save yourself the trouble and put this into an init script. This way you won’t have to repeat this pain.
Input Parameters
df: Input Dataframe
azureSqlStagingTable: Name of the Azure SQL Target Table
azureSqlDWTable: Name of the Azure SQL Target DW Table
lookupColumns: Pipe separated columns that uniquely defines a record in input dataframe e.g. CustomerId or CustomerId|FirstName
deltaColumn: Name of watermark column in input dataframe