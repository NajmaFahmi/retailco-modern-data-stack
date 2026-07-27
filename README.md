# RetailCo Modern Data Stack Pipeline

An end-to-end, production-grade data pipeline that transforms raw retail sales data into an analytics-ready star schema. Built on a modern data stack with containerized orchestration, automated data quality gates, and CI/CD.

<p align="left">
  <img src="https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/BigQuery-warehouse-4285F4?logo=googlebigquery&logoColor=white" alt="BigQuery"/>
  <img src="https://img.shields.io/badge/Google_Cloud_Storage-lake-4285F4?logo=googlecloud&logoColor=white" alt="GCS"/>
  <img src="https://img.shields.io/badge/Great_Expectations-data_quality-FF6310" alt="Great Expectations"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF?logo=githubactions&logoColor=white" alt="CI/CD"/>
</p>

---

## Overview

This pipeline ingests raw sales transactions, validates them against a data quality contract, transforms them at scale with Spark, loads them into BigQuery, and models them into a dimensional star schema with dbt. The entire workflow runs with a single command via Docker Compose and is continuously tested through GitHub Actions.

The architecture follows the **medallion pattern** (Bronze → Silver → Gold), separating immutable raw data from cleaned and curated layers for reprocessing safety and auditability.

---

## Architecture

```mermaid
flowchart TD
    A[Raw Sales CSV] -->|convert to Parquet| B[GCS Bronze<br/>immutable raw]
    B --> C{Great Expectations<br/>data quality gate}
    C -->|pass| D[Spark Transform<br/>clean · enrich · aggregate]
    C -->|fail| X[Pipeline halts]
    D --> E[GCS Silver<br/>cleaned + aggregated]
    E --> F[BigQuery<br/>native tables]
    F --> G[dbt<br/>star schema modeling]
    G --> H[Gold Layer<br/>fact + dimensions]

    style B fill:#f4a340,stroke:#333,color:#000
    style E fill:#f4a340,stroke:#333,color:#000
    style C fill:#ff6310,stroke:#333,color:#fff
    style D fill:#e25a1c,stroke:#333,color:#fff
    style F fill:#4285f4,stroke:#333,color:#fff
    style G fill:#ff694b,stroke:#333,color:#fff
    style H fill:#639922,stroke:#333,color:#fff
    style X fill:#cc2020,stroke:#333,color:#fff
```

---

## Data Model

The Gold layer implements a star schema optimized for analytical queries: a central fact table surrounded by conformed dimensions.

```mermaid
erDiagram
    fact_sales }o--|| dim_city : city_id
    fact_sales }o--|| dim_product : product_id
    fact_sales }o--|| dim_date : date_id

    fact_sales {
        int order_id
        int customer_id
        int city_id FK
        int product_id FK
        int date_id FK
        numeric amount
        int quantity
        numeric total_value
        string order_size
    }
    dim_city {
        int city_id PK
        string city_name
    }
    dim_product {
        int product_id PK
        string product_name
        numeric avg_price
    }
    dim_date {
        int date_id PK
        date order_date
        int year
        int month
        int day
        string day_of_week
    }
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Storage | Google Cloud Storage | Data lake (Bronze / Silver layers) |
| Processing | Apache Spark | Distributed transformation and aggregation |
| Warehouse | BigQuery | Analytical query engine |
| Modeling | dbt | Star schema transformation and testing |
| Data Quality | Great Expectations | Validation gate before processing |
| Orchestration | Docker Compose | Single-command pipeline execution |
| CI/CD | GitHub Actions | Automated testing and branch protection |
| File Format | Parquet | Columnar, compressed, schema-aware storage |

---

## Pipeline Stages

**1 · Ingestion** — Raw CSV is converted to Parquet and uploaded to the GCS Bronze layer as immutable raw data.

**2 · Data Quality** — Great Expectations validates the Bronze layer against seven expectations (non-null keys, positive amounts, valid category sets). A failed check halts the pipeline before any processing occurs.

**3 · Transformation** — Spark reads Bronze, cleans and enriches transactions (standardization, derived columns), aggregates by city and product, and writes two Silver datasets.

**4 · Load** — Silver Parquet is natively loaded into BigQuery with idempotent overwrite semantics.

**5 · Modeling** — dbt builds a staging view and materializes a star schema (one fact, three dimensions) with automated uniqueness and not-null tests.

---

## Data Quality

Validation runs as the first stage of the pipeline, enforcing a data contract before any compute is spent:

| Expectation | Column | Rule |
|-------------|--------|------|
| Not null | `order_id` | Every transaction has an identifier |
| Unique | `order_id` | No duplicate transactions |
| Not null | `amount` | Price is always present |
| Positive | `amount` | No zero or negative prices |
| Positive | `quantity` | No zero or negative quantities |
| In set | `city` | Only known cities |
| In set | `product` | Only known products |

If any expectation fails, the process exits with a non-zero code, and downstream stages never run.

---

## CI/CD

Every push and pull request triggers two automated jobs via GitHub Actions:

- **Python Syntax Check** — flags critical syntax and logic errors across all scripts.
- **dbt Build & Test** — rebuilds the star schema from the latest code and runs all dbt tests against BigQuery.

The `master` branch is protected: merges are blocked unless both checks pass, ensuring the main branch is always in a working state.

---

## Running the Pipeline

**Prerequisites:** Docker, a GCP service account key with BigQuery and GCS access.

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Run the entire pipeline with one command
docker compose up --build
```

Services execute in dependency order — each stage runs only if the previous one succeeds:

```
data_quality → spark_transform → bigquery_load → dbt_run
```

---

## Project Structure

```
retailco-modern-data-stack/
├── data/raw/              # Source data and generator
├── ingestion/            # CSV to GCS Bronze (Parquet)
├── data_quality/         # Great Expectations validation
├── spark/                # Bronze to Silver transformation
├── load/                 # Silver to BigQuery loader
├── dbt/retailco/         # Star schema models and tests
├── Dockerfile            # Pipeline image definition
├── docker-compose.yml    # Orchestration
└── .github/workflows/    # CI/CD pipeline
```

---

## Key Engineering Decisions

- **Medallion architecture** separates immutable raw data from cleaned layers, enabling safe reprocessing and audit trails.
- **Parquet over CSV** for columnar storage, compression, and predicate pushdown.
- **Native BigQuery load** over external tables, prioritizing query performance for repeated dbt runs.
- **Data quality as a gate**, not an afterthought — bad data is rejected before compute is spent.
- **Idempotent writes** throughout (overwrite semantics), so re-running the pipeline produces consistent results.
