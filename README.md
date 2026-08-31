# Azure Synapse Analytics – End-to-End Data Engineering Project

An end-to-end **Azure Synapse Analytics data engineering solution** demonstrating the complete data integration and transformation lifecycle using **Azure Synapse Pipelines, Azure Data Lake Storage Gen2, Mapping Data Flow, Serverless SQL, External Tables, Database-Scoped Credentials, and Managed Identity**.

The project ingests order data from CSV files, stores the raw data in a **Bronze layer**, applies cleansing and enrichment in the **Silver layer**, and creates dimensional and fact tables in the **Gold layer** for analytical workloads.

---

## Architecture

```text
                         SOURCE LAYER
                  ┌───────────────────────┐
                  │   CSV Source Files    │
                  │                       │
                  │ order_day0.csv        │
                  │ order_day1.csv        │
                  └───────────┬───────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │   Azure Synapse       │
                  │      Pipeline         │
                  │                       │
                  │    Copy Activity      │
                  └───────────┬───────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │    BRONZE LAYER       │
                  │       ADLS Gen2       │
                  │                       │
                  │    Raw → Parquet      │
                  └───────────┬───────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │   MAPPING DATA FLOW   │
                  │                       │
                  │ • Filter              │
                  │ • Transformation      │
                  │ • Enrichment          │
                  │ • Column Selection    │
                  └───────────┬───────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │    SILVER LAYER       │
                  │       ADLS Gen2       │
                  │                       │
                  │   Enriched Parquet    │
                  └───────────┬───────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │ SYNAPSE SERVERLESS    │
                  │         SQL           │
                  │                       │
                  │ External Data Sources │
                  │ External Tables       │
                  └───────────┬───────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                 ▼                         ▼
       ┌─────────────────┐       ┌─────────────────┐
       │ GOLD DIMENSIONS │       │   GOLD FACT     │
       │                 │       │                 │
       │ dimCustomer     │       │   factOrders    │
       │ dimProduct      │       │                 │
       │ dimRegion       │       │ Quantity        │
       │ dimOrder        │       │ UnitPrice       │
       │                 │       │ TotalAmount     │
       └─────────────────┘       └─────────────────┘
```

---

# Project Overview

This project demonstrates how to build a modern cloud-based data engineering pipeline using **Azure Synapse Analytics**.

The solution follows a simplified **Medallion Architecture**:

```text
Bronze → Silver → Gold
```

### Bronze Layer

Raw source data is ingested from CSV files and stored in ADLS Gen2 in **Parquet format**.

### Silver Layer

Bronze data is transformed using **Mapping Data Flow**. Data is filtered, cleansed, enriched and prepared for analytical processing.

### Gold Layer

The Silver data is exposed through **Synapse Serverless SQL**, where dimension and fact tables are created to support analytical workloads.

---

# Key Features

- Azure Synapse Analytics workspace
- Synapse Pipelines
- Azure Data Lake Storage Gen2
- CSV to Parquet ingestion
- Bronze / Silver / Gold architecture
- Mapping Data Flow
- Data cleansing and enrichment
- Serverless SQL Pool
- External Data Sources
- External File Formats
- External Tables
- Database Master Key
- Database-Scoped Credentials
- Managed Identity authentication
- Dimensional Modelling
- Surrogate Keys
- Star Schema
- SQL-based fact and dimension creation
- GitHub integration

---

# Technology Stack

| Technology | Purpose |
|---|---|
| **Azure Synapse Analytics** | Data integration, transformation and analytical processing |
| **Synapse Pipelines** | Data ingestion and pipeline orchestration |
| **Azure Data Lake Storage Gen2** | Bronze and Silver data storage |
| **Mapping Data Flow** | Data transformation and enrichment |
| **Serverless SQL Pool** | Querying lake data and creating external tables |
| **Parquet** | Columnar storage format |
| **Managed Identity** | Secure authentication to Azure Storage |
| **GitHub** | Version control for Synapse artifacts |

---

# Repository Structure

```text
Azure-Synapse-Project/
│
├── SourceFiles/
│   ├── order_day0.csv
│   └── order_day1.csv
│
├── credential/
│   └── WorkspaceSystemIdentity.json
│
├── dataflow/
│   └── enrichedSilverLayer.json
│
├── dataset/
│   ├── BronzeLayerData.json
│   ├── DumpToBronzeParquet.json
│   ├── SinkToSilverLayer.json
│   └── SourceCSV.json
│
├── integrationRuntime/
│   └── AutoResolveIntegrationRuntime.json
│
├── linkedService/
│   ├── azuresynapse56-WorkspaceDefaultSqlServer.json
│   ├── azuresynapse56-WorkspaceDefaultStorage.json
│   └── synapseSilverSQLScript.json
│
├── pipeline/
│   ├── ExternaResource.json
│   ├── rawDataInjectionBrozeLayer.json
│   └── SilverLayerInjection.json
│
├── sqlscript/
│   ├── DimensionTables.json
│   ├── FactOrders.json
│   ├── externalResourceScript.json
│   ├── masterKey.json
│   ├── Practice.json
│   ├── RenameColName.json
│   ├── schema.json
│   └── silverTable.json
│
└── publish_config.json
```

---

# End-to-End Data Flow

```text
SOURCE
  │
  ▼
CSV Files
  │
  ▼
Azure Synapse Pipeline
  │
  ▼
Copy Activity
  │
  ▼
BRONZE LAYER
ADLS Gen2
CSV → Parquet
  │
  ▼
Mapping Data Flow
  │
  ├── Filter UnitPrice > 100
  ├── Split CustomerName
  └── Select Required Columns
  │
  ▼
SILVER LAYER
ADLS Gen2
Enriched Parquet
  │
  ▼
Synapse Serverless SQL
  │
  ├── Silver External Table
  │
  ├── Gold Dimensions
  │     ├── dimCustomer
  │     ├── dimProduct
  │     ├── dimRegion
  │     └── dimOrder
  │
  └── Gold Fact
        └── factOrders
```

---

# 1. Source Layer

The project uses two CSV files as the source dataset:

```text
order_day0.csv
order_day1.csv
```

The source data represents order transactions containing information related to:

- Orders
- Customers
- Products
- Regions
- Pricing
- Quantity
- Total order amount

### Source Columns

```text
OrderID
OrderDate
CustomerID
CustomerName
CustomerEmail
ProductID
ProductName
ProductCategory
RegionID
RegionName
Country
Quantity
UnitPrice
TotalAmount
```

The `SourceCSV` dataset is configured as a **DelimitedText** dataset with an explicit schema.

---

# 2. Bronze Layer

The Bronze layer is responsible for storing the raw ingested data with minimal transformation.

The pipeline:

```text
CSV
  │
  ▼
Copy Activity
  │
  ▼
ADLS Gen2
  │
  ▼
Parquet
```

The `rawDataInjectionBrozeLayer` pipeline uses an ADF/Synapse **Copy Activity** to read the CSV source and write the data into ADLS Gen2 in Parquet format.

The pipeline performs source-to-sink mappings and type conversion for fields such as:

- Integer IDs
- Dates
- Customer information
- Product information
- Quantity
- Price
- Total Amount

### Bronze Layer Purpose

The Bronze layer provides the raw landing area for downstream processing.

---

# 3. Silver Layer

The Silver layer contains transformed and enriched data.

The `SilverLayerInjection` pipeline executes the:

```text
enrichedSilverLayer
```

Mapping Data Flow.

The overall flow is:

```text
Bronze Parquet
      │
      ▼
Mapping Data Flow
      │
      ├── Filtering
      ├── Enrichment
      ├── Transformation
      └── Column Selection
      │
      ▼
Silver Parquet
      │
      ▼
Silver External Table
```

---

# Silver Layer Transformations

## Filter High-Value Orders

The project filters records using:

```text
UnitPrice > 100
```

Only records matching this condition continue through the transformation flow.

---

## Customer Name Transformation

The Mapping Data Flow splits the:

```text
CustomerName
```

column into:

```text
FirstName
LastName
```

This converts a single customer name field into separate analytical attributes.

---

## Column Selection

Only the required business columns are selected before the Silver data is written.

---

## Silver Output

The transformed data is stored in **Parquet format**.

```text
Bronze Parquet
      │
      ▼
Filter UnitPrice > 100
      │
      ▼
Customer Name Transformation
      │
      ▼
Select Required Columns
      │
      ▼
Silver Parquet
```

---

# 4. Synapse Serverless SQL

The project uses **Synapse Serverless SQL** to query data directly from ADLS Gen2.

The Serverless SQL layer is used to create:

- Database Master Key
- Database-Scoped Credential
- External Data Sources
- External File Format
- External Tables

This provides a SQL-based analytical layer over the data stored in the data lake.

---

# 5. Database Master Key

The project contains a dedicated SQL script to create a database master key.

Example:

```sql
CREATE MASTER KEY
ENCRYPTION BY PASSWORD = '<Create master key>';
```

The repository uses a placeholder so that an actual password is not stored in source control.

The real password should be configured directly in the Azure environment.

---

# 6. Database-Scoped Credential

A database-scoped credential is created using Managed Identity:

```sql
CREATE DATABASE SCOPED CREDENTIAL my_credentials
WITH IDENTITY = 'Managed Identity';
```

This allows Synapse Serverless SQL to authenticate to ADLS Gen2 without storing a storage account access key in the repository.

---

# 7. External Data Sources

The project dynamically creates external data sources for the Silver and Gold layers.

```text
Synapse Serverless SQL
        │
        ├── silver_layer
        │
        └── gold_layer
```

The external resource procedure accepts the storage account URL as a parameter and creates the appropriate external data sources.

---

# 8. External File Format

The project defines Parquet as the external file format:

```sql
CREATE EXTERNAL FILE FORMAT parquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);
```

This allows Serverless SQL to query Parquet files directly from the data lake.

---

# 9. Silver External Table

The project creates:

```text
silver.silverTable
```

This external table points to the enriched Silver Parquet data.

### Silver Table Columns

```text
OrderID
OrderDate
CustomerID
FirstName
LastName
CustomerEmail
ProductID
ProductName
ProductCategory
RegionID
RegionName
Country
Quantity
UnitPrice
TotalAmount
```

The table is created conditionally using `IF NOT EXISTS`.

---

# 10. Gold Layer

The Gold layer converts the Silver data into a dimensional model suitable for analytical queries.

The Gold layer contains:

### Dimension Tables

```text
dimCustomer
dimProduct
dimRegion
dimOrder
```

### Fact Table

```text
factOrders
```

---

# 11. Gold Dimension Tables

## dimCustomer

Contains unique customer records.

```text
CustomerID
FirstName
LastName
CustomerEmail
```

A surrogate key is generated:

```text
DimCustomerKey
```

---

## dimProduct

Contains product-related information:

```text
ProductID
ProductName
ProductCategory
```

Surrogate key:

```text
DimProductKey
```

---

## dimRegion

Contains region information:

```text
RegionID
RegionName
Country
```

Surrogate key:

```text
DimRegionKey
```

---

## dimOrder

Contains descriptive order-level attributes.

Surrogate key:

```text
DimOrderKey
```

---

# 12. Surrogate Keys

The Gold dimension tables use `ROW_NUMBER()` to generate surrogate keys.

For example:

```sql
ROW_NUMBER() OVER (ORDER BY CustomerID)
```

This creates analytical keys such as:

```text
DimCustomerKey
DimProductKey
DimRegionKey
DimOrderKey
```

---

# 13. Gold Fact Table

The Gold fact table is:

```text
gold.factOrders
```

It is created by joining the Silver data with the Gold dimension tables.

### Fact Foreign Keys

```text
DimCustomerKey
DimProductKey
DimRegionKey
DimOrderKey
```

### Measures

```text
Quantity
UnitPrice
TotalAmount
```

---

# Star Schema

The final analytical model follows a star-schema-style design:

```text
                         dimCustomer
                              │
                              │
                              ▼
                         factOrders
                        /    │     \
                       /     │      \
                      ▼      ▼       ▼
                dimProduct dimRegion dimOrder
```

This model separates:

- **Measures** in the fact table
- **Descriptive attributes** in dimension tables

making the data easier to consume for analytical workloads and BI reporting.

---

# Pipeline Design

## rawDataInjectionBrozeLayer

Purpose:

> Ingest CSV source data into the Bronze layer as Parquet.

```text
Source CSV
    │
    ▼
Copy Activity
    │
    ▼
Bronze Parquet
```

---

## SilverLayerInjection

Purpose:

> Transform Bronze data and create the Silver external table.

```text
Bronze Parquet
      │
      ▼
Mapping Data Flow
      │
      ├── Filter
      ├── Customer Name Transformation
      └── Column Selection
      │
      ▼
Silver Parquet
      │
      ▼
SQL Script
      │
      ▼
silver.silverTable
```

The pipeline executes the Mapping Data Flow first and executes the SQL operation after successful transformation.

---

# Managed Identity and Authentication

The project uses **Managed Identity** for Synapse-to-ADLS authentication.

```text
Azure Synapse
      │
      │ Managed Identity
      ▼
ADLS Gen2
```

This approach avoids storing storage account access keys in SQL scripts.

The database-scoped credential is configured as:

```sql
IDENTITY = 'Managed Identity'
```

---

# Security Practices

The project is designed to avoid storing live credentials in the repository.

The following values should never be committed to GitHub:

```text
Passwords
Storage Account Access Keys
SAS Tokens
API Keys
Client Secrets
Connection Strings
Real Master-Key Passwords
```

Instead, use:

```text
Managed Identity
Azure Key Vault
Environment Variables
Secure Azure Configuration
```

The repository contains placeholders where environment-specific secret values are required.

---

# Data Model

The project uses an order-management domain.

## Fact Table

### factOrders

Contains measurable business values:

```text
Quantity
UnitPrice
TotalAmount
```

and foreign keys to the dimensions:

```text
DimCustomerKey
DimProductKey
DimRegionKey
DimOrderKey
```

---

## Dimension Tables

### dimCustomer

```text
CustomerID
FirstName
LastName
CustomerEmail
DimCustomerKey
```

### dimProduct

```text
ProductID
ProductName
ProductCategory
DimProductKey
```

### dimRegion

```text
RegionID
RegionName
Country
DimRegionKey
```

### dimOrder

Contains order-level descriptive attributes and:

```text
DimOrderKey
```

---

# Key Azure Synapse Components Used

| Component | Purpose |
|---|---|
| **Synapse Workspace** | Central analytics and engineering environment |
| **Pipeline** | Data movement and orchestration |
| **Copy Activity** | CSV to Parquet ingestion |
| **Mapping Data Flow** | Data cleansing and transformation |
| **ADLS Gen2** | Data lake storage |
| **Serverless SQL Pool** | SQL-based analytics over lake data |
| **External Table** | SQL representation of lake data |
| **External Data Source** | Connection between SQL and ADLS |
| **External File Format** | Defines Parquet storage format |
| **Database-Scoped Credential** | Secure authentication to storage |
| **Managed Identity** | Passwordless authentication |
| **GitHub** | Source control and collaboration |

---

# Prerequisites

To reproduce this project, you need:

- Azure subscription
- Azure Synapse Analytics workspace
- Azure Data Lake Storage Gen2 account
- Synapse Serverless SQL / Built-in SQL Pool
- Synapse Studio access
- GitHub account
- Appropriate Azure RBAC permissions

---

# Deployment and Setup

## Step 1 — Create Azure Resources

Create:

```text
Azure Synapse Workspace
ADLS Gen2 Storage Account
```

---

## Step 2 — Configure Managed Identity

Enable the Synapse workspace Managed Identity.

Grant the required permissions on the ADLS Gen2 storage account.

---

## Step 3 — Create Data Lake Structure

Create the required filesystems and folders.

Example:

```text
sourcefiles/
bronze/
silver/
gold/
```

---

## Step 4 — Upload Source Data

Upload:

```text
order_day0.csv
order_day1.csv
```

to the configured source location.

---

## Step 5 — Configure Linked Services

Configure the required:

- Storage linked service
- SQL linked service
- Integration Runtime

---

## Step 6 — Deploy Datasets

Deploy the JSON artifacts from:

```text
dataset/
```

---

## Step 7 — Deploy Mapping Data Flow

Deploy:

```text
enrichedSilverLayer
```

from:

```text
dataflow/
```

---

## Step 8 — Deploy Pipelines

Deploy:

```text
rawDataInjectionBrozeLayer
SilverLayerInjection
```

from:

```text
pipeline/
```

---

## Step 9 — Configure External Resources

Execute the external resource SQL configuration to create:

```text
Database-Scoped Credential
Silver External Data Source
Gold External Data Source
Parquet File Format
```

---

## Step 10 — Create Silver Table

Execute:

```text
sqlscript/silverTable.json
```

to create:

```text
silver.silverTable
```

---

## Step 11 — Create Gold Dimensions

Execute:

```text
sqlscript/DimensionTables.json
```

to create:

```text
gold.dimCustomer
gold.dimProduct
gold.dimRegion
gold.dimOrder
```

---

## Step 12 — Create Gold Fact

Execute:

```text
sqlscript/FactOrders.json
```

to create:

```text
gold.factOrders
```

---

## Step 13 — Validate the Data

Use Synapse Serverless SQL to query:

```text
silver.silverTable
gold.dimCustomer
gold.dimProduct
gold.dimRegion
gold.dimOrder
gold.factOrders
```
and validate the complete data flow.

---
# Connecting Azure Synapse Analytics to Power BI

The Gold layer of the Synapse project can be connected to **Power BI** for reporting and visualization.

The recommended architecture is:

```text
Source Data
    ↓
Synapse Pipelines
    ↓
Bronze Layer
    ↓
Mapping Data Flow
    ↓
Silver Layer
    ↓
Synapse Serverless SQL
    ↓
Gold Tables
    ↓
Power BI
```

## Step 1: Get the Synapse SQL Endpoint

1. Open the **Azure Portal**.
2. Navigate to **Azure Synapse Workspace**.
3. Open your Synapse workspace.
4. On the **Overview** page, locate the **Workspace SQL endpoint**.
5. Copy the SQL endpoint.

It will look similar to:

```text
<workspace-name>.sql.azuresynapse.net
```

> **Note:** Use the Synapse SQL endpoint for Power BI. Do not use the ADLS Gen2 storage URL.

---

## Step 2: Open Power BI Desktop

1. Open **Power BI Desktop**.
2. Select **Home → Get Data**.
3. Search for:

```text
Azure Synapse Analytics SQL
```

4. Select **Azure Synapse Analytics SQL**.
5. Click **Connect**.

---

## Step 3: Enter the Synapse Connection Details

Enter the Synapse SQL endpoint obtained in Step 1.

Example:

```text
Server:
<workspace-name>.sql.azuresynapse.net
```

Enter the Synapse SQL database name.

Example:

```text
Database:
synapseDWH
```

Then click **OK**.

Power BI will prompt for authentication.

---

## Step 4: Authenticate and Select the Tables

1. Select the appropriate authentication method, preferably **Microsoft Entra ID / Organizational Account**.
2. Sign in using an account that has permission to access the Synapse SQL endpoint and database.
3. In the **Navigator** window, locate the Gold tables.
4. Select the required tables:

```text
gold.dimCustomer
gold.dimProduct
gold.dimRegion
gold.dimOrder
gold.factOrders
```

5. Select **Transform Data** if you want to review or modify the data before loading it.
6. Otherwise, select **Load** to import the tables into Power BI.

The resulting Power BI model can then be used to create relationships, DAX measures, reports, and dashboards.
---

# End-to-End Execution

```text
              SOURCE
                │
                ▼
        order_day0.csv
        order_day1.csv
                │
                ▼
       Synapse Copy Activity
                │
                ▼
        BRONZE – ADLS Gen2
                │
                │ Parquet
                ▼
     enrichedSilverLayer
        Mapping Data Flow
                │
        ┌───────┼────────┐
        │       │        │
      Filter  Enrich   Select
        │       │        │
        └───────┼────────┘
                │
                ▼
        SILVER – ADLS Gen2
                │
                ▼
        Serverless SQL
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
   Dimensions        FactOrders
        │                │
        └───────┬────────┘
                ▼
          GOLD LAYER
                │
                ▼
          BI / Analytics
```

---

# Learning Outcomes

This project provides hands-on experience with:

- Azure Synapse Analytics
- Synapse Pipelines
- Azure Data Lake Storage Gen2
- Data ingestion
- Copy Activity
- Mapping Data Flow
- Bronze/Silver/Gold architecture
- Parquet storage
- Serverless SQL
- External Tables
- External Data Sources
- External File Formats
- Database Master Key
- Database-Scoped Credentials
- Managed Identity
- Dimensional Modelling
- Surrogate Keys
- Star Schema
- SQL-based transformations
- Git-based Synapse development

---

# Project Highlights

### Data Engineering

- Designed an end-to-end data pipeline using Azure Synapse
- Implemented multi-layer data processing
- Converted raw CSV data into optimized Parquet format
- Built transformation pipelines using Mapping Data Flow

### Data Lake

- Implemented Bronze and Silver layers using ADLS Gen2
- Used Parquet for efficient analytical storage
- Organized data according to Medallion Architecture principles

### Data Warehouse / Analytics

- Created external tables using Synapse Serverless SQL
- Built dimension and fact tables
- Generated surrogate keys
- Implemented star-schema modelling

### Security

- Used Managed Identity for Azure resource authentication
- Implemented Database-Scoped Credentials
- Avoided hard-coded production secrets in source control

### DevOps

- Managed Synapse artifacts using GitHub
- Maintained pipelines, dataflows, datasets and SQL scripts in source control

---

# Author

**Dipak**

GitHub: [CodeWithDipak](https://github.com/CodeWithDipak)

---

# License

No license file is currently included in this repository.

If this project is intended for public reuse, consider adding an appropriate open-source license.
