# Logistics and Delivery Performance Analysis

![Hadoop](https://img.shields.io/badge/-Hadoop-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/-Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Hive](https://img.shields.io/badge/-Hive-FDEE21?style=flat-square&logo=apachehive&logoColor=black)
![Sqoop](https://img.shields.io/badge/-Sqoop-2B4A87?style=flat-square)
![MySQL](https://img.shields.io/badge/-MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Bash](https://img.shields.io/badge/-Bash-4EAA25?style=flat-square&logo=gnubash&logoColor=white)
![Cron](https://img.shields.io/badge/-Cron-black?style=flat-square&logo=linux&logoColor=white)

## Motivation & Project Overview

Delivery performance is one of the highest-impact, least-visible parts of an e-commerce operation — customers rarely see *why* an order was late, only that it was. Raw order and logistics records aren't built to answer that question directly: dates are scattered across timestamps, sellers ship multiple items per order, and "late" isn't even a stored field, it has to be derived.

This project builds an end-to-end batch data engineering pipeline on a Hadoop/Spark/Hive stack that:

- Ingests the Olist Brazilian e-commerce dataset (customers, sellers, orders, order items) from MySQL into HDFS via Sqoop.
- Cleans and standardizes raw records into a validated Silver layer using PySpark.
- Models the data into a Kimball-style star schema (Gold layer) purpose-built to answer one question: **did the order arrive on time, and why or why not?**
- Serves the warehouse through Hive external tables for KPI analysis.
- Runs unattended, end-to-end, on a schedule via a bash + cron orchestration layer.

The result is a reproducible logistics analytics warehouse capable of measuring on-time delivery rate, shipping cost efficiency, and regional delivery performance.

## Problem Description

The core problem this project solves is turning a transactional e-commerce dataset — built for recording purchases, not for measuring logistics performance — into a warehouse that can answer delivery-performance questions reliably.

Key issues addressed:

- `order_delivered_carrier_date` and `order_delivered_customer_date` are frequently null for orders that were shipped, cancelled, or are still in transit — these are legitimate business states, not data quality failures, and had to be handled without discarding valid rows.
- The raw `"null"` string (not a true SQL/Spark null) was silently present in several date columns and had to be explicitly detected and converted.
- Orders can have multiple items and, rarely, multiple sellers — the warehouse grain needed a clear, justified rule for resolving one logistics record per order.
- One malformed seller record (a corrupted row with non-hex characters in `seller_id`) was identified via regex validation and excluded from the Silver layer only, preserving the Raw layer as an unmodified source of truth.

Business decisions implemented:

- **Grain:** one row per delivered order in the fact table — orders without a valid delivery date are excluded from the fact table by design, since lateness can't be evaluated without one.
- **Primary seller resolution:** for the small share of orders (~1.3%) involving more than one seller, the seller with the highest freight cost is selected as the order's primary seller/warehouse (`MAX_BY(seller_id, freight_value)`), validated against the full order_items dataset before being applied.
- **Lateness semantics:** `is_late_delivery` is `NULL`, not `0`, when there's no delivery date — null means "not evaluable," not "on time."

## Architecture Summary

The project follows a batch ELT architecture on a single-node Hadoop cluster:

1. **Source:** Brazilian E-Commerce Public Dataset by Olist (CSV, via Kaggle).
2. **Staging:** CSVs loaded into a MySQL `olist` database (4 tables: `customers`, `sellers`, `orders`, `order_items`).
3. **Ingestion:** Apache Sqoop imports each table from MySQL into HDFS (`/user/hadoop/commerce_storage/`).
4. **Silver Layer:** PySpark cleans, standardizes, and validates each table (null handling, dedup, regex validation, timestamp casting), writing Parquet to `/user/hadoop/commerce_silver/`.
5. **Gold Layer:** PySpark builds a Kimball star schema — `dim_customer`, `dim_seller`, `dim_date`, and `fact_logistics` — writing Parquet to `/user/hadoop/commerce_gold/`.
6. **Serving Layer:** Hive external tables over the Gold Parquet paths, queried for KPI reporting.
7. **Orchestration:** a bash script chains all of the above (Sqoop → Silver → Gold) and is scheduled via cron.

*(Pipeline architecture diagram — see `data_model/`.)*

## Data Warehouse Design

Designed following the Kimball four-step process: choose the business process → declare the grain → identify dimensions → identify facts.

**Business process:** Order delivery performance — not the purchase itself, but whether the order reached the customer on time.

**Grain:** One row per order with a valid logistics record (`order_status = 'delivered'` and a non-null `order_delivered_customer_date`).

**Dimensions:**
- `dim_customer` — customer delivery location (city/state/zip), Type 1 (each `customer_id` is already a point-in-time snapshot in the source data, so there's no history to preserve).
- `dim_seller` — seller/warehouse location, used in place of a `Dim_Warehouse`, Type 1 (single static batch load, no repeated ingestion to track changes against).
- `dim_date` — a generated calendar table spanning the full order date range, referenced twice from the fact table (`order_date_key` and `delivery_date_key`) as a conformed, role-playing dimension.

**Facts (`fact_logistics`):**

| Measure | Type | Business Purpose |
|---|---|---|
| `delivery_time_delta_days` | Non-additive | Actual vs. estimated delivery — primary logistics KPI |
| `is_late_delivery` | Binary flag | Efficiency identification |
| `order_to_delivery_days` | Non-additive | Total customer-facing lead time |
| `approval_to_ship_days` | Non-additive | Seller/warehouse processing time before carrier handover |
| `shipping_cost` | Additive | Aggregated freight cost per order |
| `total_order_price` | Additive | Aggregated product value per order |

A **star schema** (one fact, three dimensions) was chosen over a snowflake or galaxy design — the dimensions here don't have natural sub-hierarchies worth normalizing out, and a single fact table is sufficient for this business process.

## Batch Ingestion (Sqoop)

Each of the four Olist tables is imported independently from MySQL into HDFS:

```bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/olist \
  --username <user> --password <password> \
  --table <table_name> \
  --target-dir /user/hadoop/commerce_storage/<table_name> \
  --delete-target-dir \
  --m 1
```

`--delete-target-dir` makes every import idempotent — re-running an import for a given table fully replaces its HDFS output rather than appending duplicates.

## Transformations (PySpark)

**Silver layer** (`spark/scripts/silver_transformations.py`):
- Manual schema definition on read (Sqoop output has no headers).
- Detection and conversion of the literal `"null"` string to true nulls across all date columns.
- Derivation of `delivery_days` and `is_late` on `orders`, with explicit null-safe handling.
- Regex-based validation and removal of one malformed `sellers` record.
- Standardization (trim/case normalization) on customer and seller city/state fields, with zip codes deliberately kept as `StringType` to preserve leading zeros.
- Deduplication on natural/composite keys (`customer_id`; `seller_id`; `order_id` + `order_item_id`).

**Gold layer** (`spark/scripts/gold_transformations.py`):
- `dim_customer` / `dim_seller` — direct, validated pass-throughs from Silver.
- `dim_date` — generated via `sequence()` + `explode()` across the full order date range, with standard calendar attributes derived from `date_format`/`year`/`quarter`/etc.
- `fact_logistics` — aggregates `order_items` to order grain (primary seller via `MAX_BY`, summed freight/price), filters to delivered orders with a valid delivery date, joins to both dimensions, and derives all six logistics measures.

The full exploratory process — EDA, the missing-value investigation, the seller row-count discrepancy, and the reasoning behind each decision — is documented in `spark/Olist_Data_Processing_notebook.ipynb`. The two `.py` scripts are the clean, production versions of that same logic, meant to be run via `spark-submit`.

## Serving Layer (Hive)

Four external tables are created in Hue's Hive editor, pointing directly at the Gold-layer Parquet paths — no data movement, Hive reads what Spark already wrote:

```sql
CREATE EXTERNAL TABLE fact_logistics (...)
STORED AS PARQUET
LOCATION '/user/hadoop/commerce_gold/fact_logistics';
```

See `hive/create_tables.sql` for full DDL and `hive/kpi_queries.sql` for the reporting queries, including:

- On-Time Delivery Rate %
- Late Delivery Rate %
- Average Delivery Time
- Freight / Shipping Cost Ratio
- Delivery Performance by Region

## Orchestration

`orchestration/run_pipeline.sh` chains the entire pipeline — 4 Sqoop imports → Silver transformation → Gold transformation — in sequence, using `set -e` so the script stops immediately if any stage fails rather than continuing on incomplete data. Every run produces a timestamped log capturing full stdout/stderr from every step.

The script is registered via `crontab -e` for scheduled, unattended execution.

**Why bash + cron instead of Airflow or NiFi:** Airflow requires its own persistent scheduler/webserver/metadata-DB stack, which was too much additional load to risk on a VM already running the full Hadoop stack on constrained RAM this close to the deadline. NiFi is built for continuously arriving data, not a one-time batch pull, and its processor model isn't a natural fit for the multi-table aggregation the Gold layer needs. A bash script scheduled via cron demonstrates the same core idea — automated, unattended, scheduled execution — with far less setup risk.

Getting the script working correctly under cron (as opposed to running manually) surfaced three real environment issues — `PATH` not including Sqoop's binary, the MySQL JDBC driver not being on Sqoop's classpath, and `HADOOP_CONF_DIR` not being set, causing Spark to default to the local filesystem instead of HDFS — all traced back to cron's non-interactive shell not loading the same environment as an interactive terminal. Full details in the project documentation.

## Dashboard

![Dashboard Screenshot](dashboard/Screenshot%202026-09-04%20162631.png)

## Repository Structure

```text
Logistics Performance Pipeline/
├── spark/
│   ├── Olist_Data_Processing_notebook.ipynb
│   └── scripts/
│       ├── silver_transformations.py
│       └── gold_transformations.py
├── hive/
│   ├── create_tables.sql
│   └── kpi_queries.sql
├── orchestration/
│   └── run_pipeline.sh
├── data_model/
│   └── (ERD, Kimball design docs, pipeline architecture diagram)
└── dashboards/
   └── (pending)
 
```

## Setup & Reproducibility

This project was built and run on a single-node CentOS 7 VM with a pre-installed Hadoop/Spark/Hive/Sqoop stack (Hadoop 3.3.1, Spark 3.1.2, Hive 3.1.2, Sqoop 1.4.7). It is not cloud-deployed — reproducing it requires an equivalent local Hadoop ecosystem setup rather than a cloud account.

### Prerequisites

- Hadoop 3.x (HDFS + YARN) running locally or on a VM
- Apache Spark 3.x with PySpark
- Apache Hive 3.x (with Hue, optional but recommended for the serving layer)
- Apache Sqoop 1.4.7
- MySQL with the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) loaded

### 1. Load the source data into MySQL

Create the `olist` database and its four tables, then load each CSV via `LOAD DATA LOCAL INFILE`. See `data_model/` for the full table DDL.

### 2. Run the ingestion + transformation pipeline

Either run each stage manually:

```bash
# Sqoop import (repeat per table)
sqoop import --connect jdbc:mysql://localhost:3306/olist \
  --username <user> --password <password> \
  --table customers --target-dir /user/hadoop/commerce_storage/customers \
  --delete-target-dir --m 1

# Silver layer
spark-submit spark/scripts/silver_transformations.py

# Gold layer
spark-submit spark/scripts/gold_transformations.py
```

...or run the whole pipeline end-to-end with the orchestration script:

```bash
bash orchestration/run_pipeline.sh
```

### 3. Create the Hive tables

In Hue's Hive editor (or `hive`/`beeline` CLI), run `hive/create_tables.sql` to register the four external tables over the Gold-layer Parquet paths.

### 4. Run the KPI queries

Run the queries in `hive/kpi_queries.sql` against the Hive tables to reproduce the reporting metrics.

### 5. (Optional) Schedule the pipeline

```bash
crontab -e
# 0 2 * * * /path/to/orchestration/run_pipeline.sh
```

## Team

- **Abdelrahman Mohamed**
- **Mahmoud Ali**
- **Hady El Fadaly**
- **Mariam Tarek**

## Future Enhancements

- Add data quality tests (null/uniqueness/referential integrity checks) as an automated step in the pipeline rather than manual notebook validation.
- Extend `dim_seller`/`dim_customer` to SCD Type 2 if the pipeline were adapted to run against a live, repeatedly-refreshed source rather than a static dataset.
- Add a lightweight CI check that lints the PySpark scripts on push.
