# Walmart Modern Data Engineering Lakehouse

> An end-to-end modern data engineering project that moves retail data from an **agentic Neon PostgreSQL source and AWS S3** into a **Databricks Delta Lakehouse**, applying incremental/CDC-style ingestion, dbt transformations, data quality controls, dimensional modelling and Dockerised Apache Airflow orchestration.

## Project Overview

This project simulates a modern retail data platform built around a Walmart-style operational dataset. The objective is to demonstrate how modern data engineering components work together as one repeatable platform rather than as isolated technology exercises.

The core flow is:

> **Neon PostgreSQL → Incremental / CDC-style ingestion → Databricks Bronze → dbt Silver → Data Quality → Gold Dimensional Model**

With **AWS S3** providing cloud object storage and **Dockerised Apache Airflow** coordinating the workflow.

The project also includes an **agentic database workflow using Neon PostgreSQL and MCP**, allowing database exploration, metadata discovery, SQL generation and database operations from the development environment.

## Architecture

```text
                         ┌──────────────────────────────┐
                         │       SOURCE SYSTEM          │
                         └──────────────┬───────────────┘
                                        │
                         ┌──────────────┴──────────────┐
                         │                             │
                         ▼                             ▼
              ┌─────────────────────┐       ┌─────────────────────┐
              │ Neon PostgreSQL     │       │ AWS S3              │
              │ Operational Source  │       │ Cloud Object Store  │
              └──────────┬──────────┘       └──────────┬──────────┘
                         │                             │
                         │ Incremental / CDC-style     │
                         │ ingestion                   │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────────┐
                         │       DATABRICKS             │
                         │       + UNITY CATALOG        │
                         │                              │
                         │        BRONZE LAYER          │
                         │   Source-aligned Delta data  │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────────┐
                         │        dbt + Databricks      │
                         │                              │
                         │        SILVER LAYER          │
                         │                              │
                         │ • Technical models           │
                         │ • Incremental models         │
                         │ • Jinja / metadata-driven    │
                         │ • OBT                        │
                         │ • Data quality tests         │
                         │ • Source freshness           │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────────┐
                         │          GOLD LAYER          │
                         │                              │
                         │ • Star schema                │
                         │ • Fact tables                │
                         │ • Dimension tables           │
                         │ • SCD Type 2                 │
                         │ • dbt snapshots              │
                         │ • Ephemeral models           │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────────┐
                         │     ANALYTICS-READY DATA     │
                         └──────────────────────────────┘

       ┌───────────────────────────────────────────────────────┐
       │                    ORCHESTRATION                      │
       │       Dockerised Apache Airflow                       │
       │                                                       │
       │  Ingestion → dbt → Tests → Validation → Dependencies  │
       └───────────────────────────────────────────────────────┘

       ┌───────────────────────────────────────────────────────┐
       │                 AGENTIC DEVELOPMENT                   │
       │                                                       │
       │ VS Code → MCP → Agentic Neon PostgreSQL               │
       │                                                       │
       │ Metadata discovery | SQL | DDL | Data loading         │
       └───────────────────────────────────────────────────────┘
```

## Technology Stack

| Area | Technology | Purpose |
|---|---|---|
| Source database | **Neon PostgreSQL** | Operational retail data source |
| Agentic database access | **MCP** | AI-assisted database interaction |
| Cloud storage | **AWS S3** | Cloud object storage / data lake |
| Lakehouse | **Databricks** | Distributed processing and lakehouse platform |
| Processing | **Apache Spark / PySpark** | Distributed data processing |
| Storage format | **Delta Lake** | Reliable lakehouse storage |
| Governance | **Unity Catalog** | Data and access governance |
| Transformation | **dbt Core** | SQL transformation and modelling |
| Templating | **Jinja** | Reusable and metadata-driven SQL |
| Orchestration | **Apache Airflow** | Workflow scheduling and dependency management |
| Containerisation | **Docker / Docker Compose** | Reproducible Airflow runtime |
| Programming | **Python / SQL** | Pipeline and transformation development |
| Version control | **Git / GitHub** | Source control and project delivery |
| Development | **VS Code** | Development environment |

## Business Scenario

The simulated retail environment contains operational entities such as:

- Customers
- Stores
- Products
- Employees
- Orders
- Order Items
- Payments

The objective is to create a reliable analytical platform that can:

1. Ingest operational data incrementally.
2. Preserve source-aligned data in a Bronze layer.
3. Clean and standardise data in Silver.
4. Build a reusable integrated One Big Table.
5. Apply automated data quality validation.
6. Create a business-ready Gold dimensional model.
7. Preserve historical changes using SCD Type 2.
8. Support incremental processing and full refresh/backfill scenarios.
9. Orchestrate the workflow using Apache Airflow.
10. Keep the implementation reproducible through Git and GitHub.

## Source Layer — Neon PostgreSQL

The operational source is **Neon PostgreSQL**.

Neon provides the PostgreSQL database representing the operational retail environment. The database contains the source entities required by the downstream lakehouse pipeline.

The project also uses an **agentic database workflow through MCP**:

```text
      VS Code
         │
         ▼
        MCP
         │
         ▼
Agentic Neon PostgreSQL
   │
   ├── Metadata discovery
   ├── SQL generation
   ├── Schema inspection
   ├── DDL execution
   ├── Data loading
   └── Exploratory queries
```

This allows database operations to be performed from the development environment while the actual data engineering pipeline remains based on conventional SQL, Python, Spark, dbt and Airflow components.

## AWS S3 Data Lake

AWS S3 provides cloud object storage for the project.

The architecture separates:

- **Storage** — AWS S3
- **Processing** — Databricks
- **Transformation** — dbt
- **Orchestration** — Airflow

Databricks accesses the S3 environment through a governed external-location pattern using Unity Catalog.

```text
AWS S3
   │
   │ External Location
   ▼
Unity Catalog
   │
   ▼
Databricks
```

## Databricks Lakehouse

Databricks is the central processing and lakehouse platform.

The project uses:

- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- Bronze / Silver / Gold architecture

The lakehouse separates source-aligned ingestion from reusable transformation logic and business-facing analytical models.

## Bronze Layer

The Bronze layer is the source-aligned landing layer. Its purpose is to retain data close to the operational source structure while providing a reliable Delta Lake representation for downstream processing.

### Bronze responsibilities

- Ingest source data
- Preserve source-aligned records
- Support incremental processing
- Provide a recoverable foundation for downstream transformations
- Separate ingestion from business transformations

The Bronze layer intentionally contains minimal business logic.

## Incremental / CDC-Style Ingestion

The pipeline is designed around **incremental / CDC-style processing** rather than repeatedly rebuilding the entire dataset.

```text
Initial Load
     │
     ▼
Full source dataset
     │
     ▼
Bronze Delta

Subsequent Runs
     │
     ▼
New / changed records
     │
     ▼
Incremental processing
```

The implementation uses source keys and incremental processing logic to distinguish the initial load from subsequent processing.

### Why incremental processing?

Incremental processing is intended to reduce unnecessary processing, repeated reads and full dataset reloads while supporting repeatable scheduled execution.

> **Technical note:** This project uses the term **CDC-style/incremental ingestion**. It does not claim a specific WAL-based CDC technology such as Debezium.

## Silver Layer

The Silver layer contains cleaned, standardised and integrated datasets.

Technical Silver models cover the main source entities, including:

- Customers
- Stores
- Products
- Employees
- Orders
- Order Items

Transformations include standardisation, joins, business logic and preparation for downstream analytical modelling.

## One Big Table

A major Silver-layer component is a reusable **One Big Table (OBT)** containing **columns from all the tables**.

The OBT integrates information across:

- Orders
- Order Items
- Customers
- Products
- Employees
- Stores
- Payment / order context

A processing timestamp is also incorporated into the transformation.

The OBT provides an integrated Silver representation before the data is reshaped into the final Gold dimensional model.

## dbt Transformation Layer

dbt is used as the transformation and modelling framework on Databricks.

The project demonstrates:

- dbt Core
- dbt models
- Incremental models
- `ref()`
- Jinja
- Macros / templates
- Data tests
- Source freshness
- Snapshots
- Ephemeral models
- Full refresh
- Metadata-driven SQL generation

A key design principle is to keep transformation logic in dbt rather than turning Airflow into the transformation engine.

Airflow coordinates the workflow, dbt defines the transformation logic, and Databricks/Spark performs the underlying distributed processing.

## Incremental dbt Models

Silver transformations support both initial and subsequent processing.

```text
Initial execution
      │
      ▼
Full processing

Subsequent execution
      │
      ▼
Incremental filter
      │
      ▼
New / changed data
```

Jinja is used to conditionally apply incremental filtering logic. The same models can therefore support initial/full processing and subsequent incremental execution, with full refresh available when required.

## Metadata-Driven SQL

The project demonstrates a metadata-driven transformation pattern using **dbt + Jinja**.

```text
Metadata / Configuration
          │
          ▼
      Jinja Template
          │
          ▼
      Generated SQL
          │
          ▼
         OBT
```

Instead of embedding every relationship directly into one large transformation query, configuration influences SQL generation. This improves maintainability and allows transformation logic to be extended without rewriting the complete OBT query.

## Data Quality

Data quality is implemented as part of the transformation workflow.

The project uses dbt tests and custom SQL tests to validate areas such as:

- Primary-key integrity
- Null checks
- Model-level integrity
- Business conditions
- OBT key validation

Custom SQL tests identify rows that violate defined expectations.

```text
No violating rows
       │
       ▼
   Test passes

Violating rows
       │
       ▼
Test fails / warns
```

## Source Freshness

The project incorporates **dbt source freshness checks** to identify whether source data has been updated within an expected period.

This adds an operational reliability layer beyond simply checking whether transformation SQL executes successfully.

## Gold Layer

The Gold layer provides the business-facing analytical model.

The project implements a **dimensional star schema** containing fact and dimension models around the retail order process.

### Gold dimensions

- `dim_customers`
- `dim_products`
- `dim_stores`
- `dim_employees`
- `dim_payments`
- `dim_orders`

The Gold layer also contains an order-centric fact model.

## Star Schema

```text
                 dim_customers
                       │
                       │
dim_products ─── order fact ─── dim_stores
                       │
                       │
                 dim_employees
                       │
                       │
                  dim_orders
```

The dimensional model separates descriptive business entities into dimensions and measurable business events into fact tables.

## Slowly Changing Dimensions — Type 2

The project implements **SCD Type 2** for historical dimension management.

Instead of overwriting a changed dimension record, historical versions are retained.

```text
Customer version 1
      │
      │ attribute changes
      ▼
Customer version 2
      │
      ▼
Current version
```

This preserves historical context so facts can be interpreted against the correct dimension state at the time of the event.

The project uses **dbt snapshots** to support this historical tracking pattern.

## dbt Snapshots

Snapshots provide a mechanism for detecting changes to source/model records and maintaining historical versions. In this project they support the SCD Type 2 design of the Gold dimensional layer.

## Ephemeral Models

The project also uses **dbt ephemeral models** where appropriate. These provide reusable intermediate transformation logic without unnecessarily materialising every intermediate step as a physical database relation.

## Apache Airflow Orchestration

Apache Airflow acts as the orchestration layer. It coordinates dependencies between the major pipeline stages rather than performing the heavy distributed transformations itself.

```text
Source / ingestion
       │
       ▼
     Bronze
       │
       ▼
     Silver
       │
       ▼
 Data Quality
       │
       ▼
      Gold
       │
       ▼
   Validation
```

## Dockerised Airflow

Airflow runs in a Docker-based development environment with a custom image containing the dependencies required by the project's dbt workflow.

```text
Docker Compose
│
├── Airflow Scheduler
├── Airflow API / UI
├── Airflow Worker
├── Airflow Metadata Database
│
└── Walmart Project
      ├── DAGs
      ├── dbt Project
      └── Configuration
```

Docker provides a controlled and reproducible runtime for the orchestration layer.

## Engineering Troubleshooting

The project includes integration and debugging work rather than only a successful happy path.

Examples include:

- dbt commands unavailable inside Airflow
- Airflow dependency problems
- Missing `requirements.txt`
- Docker image rebuilds
- DAG parsing issues
- Airflow operator syntax problems
- dbt incremental model errors
- Duplicate or renamed model files
- Full-refresh requirements
- Data quality test failures
- Configuration corrections

These issues demonstrate the practical maintenance and debugging involved in integrating multiple data engineering components.

## Agentic Database Development

A distinctive part of the project is the use of an **agentic Neon PostgreSQL workflow through MCP**.

Examples of database interaction include:

- Metadata discovery
- Primary-key discovery
- SQL generation
- Schema inspection
- DDL execution
- Table creation
- CSV data loading
- Exploratory queries
- Database operations

The agentic layer assists development and database exploration while the core pipeline remains based on established data engineering components such as Spark, Delta, dbt and Airflow.

## End-to-End Execution

```text
1. Retail data exists in Neon PostgreSQL
              │
              ▼
2. Incremental / CDC-style ingestion identifies
   new or changed source data
              │
              ▼
3. Databricks lands source-aligned data into Bronze Delta
              │
              ▼
4. dbt transforms Bronze data into Silver models
              │
              ▼
5. Incremental models and metadata-driven Jinja
   transformations build the Silver layer
              │
              ▼
6. One Big Table integrates the
   major retail entities
              │
              ▼
7. dbt tests and source-freshness checks validate
   the transformation layer
              │
              ▼
8. Gold dimensional models are created
              │
              ▼
9. SCD Type 2 snapshots preserve historical
   dimension changes
              │
              ▼
10. Apache Airflow orchestrates dependencies
              │
              ▼
11. Docker provides a reproducible orchestration runtime
              │
              ▼
12. Git/GitHub provide version-controlled delivery
```

## Key Engineering Patterns

| Pattern | Implementation |
|---|---|
| Lakehouse | Databricks + Delta Lake |
| Medallion architecture | Bronze → Silver → Gold |
| Incremental processing | Incremental / CDC-style ingestion |
| ELT | dbt transformations on Databricks |
| Metadata-driven transformation | Jinja + configuration |
| Data quality | dbt tests + custom SQL tests |
| Freshness monitoring | dbt source freshness |
| Historical modelling | SCD Type 2 |
| Change tracking | dbt snapshots |
| Analytical modelling | Star schema |
| Reusable transformations | dbt models / macros / ephemeral models |
| Workflow orchestration | Apache Airflow |
| Reproducible runtime | Docker |
| Cloud storage | AWS S3 |
| Governance | Unity Catalog |
| Version control | Git / GitHub |
| AI-assisted development | MCP + agentic Neon PostgreSQL |

## Why This Architecture?

The project deliberately separates responsibilities:

- **Databricks** — distributed processing and lakehouse platform
- **Delta Lake** — transactional lakehouse storage
- **S3** — cloud object storage
- **dbt** — SQL transformation and analytical modelling
- **Airflow** — workflow orchestration and dependencies
- **Docker** — reproducible orchestration runtime
- **Neon PostgreSQL** — operational source system
- **MCP** — agentic interface for database development and exploration

This separation keeps each component focused on its role and makes the overall architecture easier to maintain and extend.

## Skills Demonstrated

### Data Engineering

`Python` `SQL` `PySpark` `Apache Spark` `Incremental Processing` `CDC Patterns`

### Lakehouse

`Databricks` `Delta Lake` `Unity Catalog` `Medallion Architecture`

### Cloud

`AWS S3`

### Analytics Engineering

`dbt Core` `Jinja` `Incremental Models` `Snapshots` `Ephemeral Models` `Data Quality`

### Data Modelling

`One Big Table` `Star Schema` `Fact Tables` `Dimension Tables` `SCD Type 2`

### Orchestration & Deployment

`Apache Airflow` `Docker` `Docker Compose`

### Development

`Git` `GitHub` `VS Code` `MCP` `Agentic Database Workflows`

## Final Notes

This project demonstrates practical Data Engineering capabilities across ingestion, cloud storage, lakehouse architecture, distributed processing, SQL transformation, data modelling, data quality, historical data management, workflow orchestration, containerisation and version control.

The architecture is designed to be adaptable to other domains such as e-commerce, banking, insurance, logistics and manufacturing by changing the source entities and business rules while retaining the underlying engineering patterns.

