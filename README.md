# Product-Focused Payment Transaction Data Platform

A resilient, production-minded ingestion and analytical transformation pipeline built on Databricks Serverless Compute and Delta Lake[cite: 1]. The system extracts payment transactions from an upstream PostgREST/Supabase REST API, enforces schema contracts, isolates defective records into a quarantine dead-letter layer, persists raw data, and builds an idempotent daily account summary data mart[cite: 1].

---

## 1. System Architecture & Pipeline Flow

The platform follows a **Medallion Data Architecture (Bronze -> Quarantine / Silver -> Gold)** with strict schema gating and incremental watermark controls[cite: 1].



                 +----------------------------+
                 |    Upstream Supabase API   |
                 +--------------+-------------+
                                |
                                | (HTTP GET with Exponential Backoff)
                                v
             +------------------------------------+
             |      Python Ingestion Engine       | <--- Watermark Filter
             |   (Pydantic Contract Validation)   |      (MAX transaction_date)
             +------------------+-----------------+
                                |
                 +--------------+--------------+
                 |                             |
       (Passed Validation)            (Defects / Duplicates)
                 |                             |
                 v                             v
      +--------------------+       +------------------------+
      |    Bronze Layer    |       |    Quarantine Layer    |
      |  (Delta Lake Raw)  |       |  (Dead-Letter Storage) |
      +----------+---------+       +------------------------+
                 |
                 | (Idempotent Delta MERGE)
                 v
      +--------------------+
      |     Gold Mart      |
      | (daily_account_    |
      |     summary)       |
      +--------------------+

```

```

### End-to-End Execution Sequence
1. **Incremental Watermark Extraction**: Queries `MAX(transaction_date)` from `payments_lakehouse.bronze_transactions` and applies a 1-hour lookback buffer to catch in-flight or late-arriving events.
2. **Resilient HTTP Extraction**: Fetches records in 100-item pages using an HTTP session configured with exponential backoff and jitter targeting status codes 429, 500, 502, 503, and 504.
3. **Data Quality Gating (Pydantic v2)**: Validates each payload against strict schema rules (regex IDs, ISO 8601 UTC timestamps with `Z` suffix, positive amounts, valid enums, and real ISO 3166-1 alpha-2 country codes via `pycountry`).
4. **Natural Key Deduplication**: Builds a composite natural key across all transaction attributes except `transaction_id`. Identical transactions with differing synthetic IDs are routed directly to quarantine.
5. **Bronze Delta Persistence**: Valid records are merged into `bronze_transactions` using an idempotent `MERGE` on `transaction_id`.
6. **Gold Mart Transformation**: Aggregates completed records into `daily_account_summary` per account and calendar date, deriving net balances, transaction counts, distinct merchants, and the top debit spend category via window functions.
7. **Automated Assertion Testing**: Enforces primary key uniqueness, net amount formula verification, and reconciliation against bronze completed records before generating CSV samples.

---

## 2. Repository & Folder Structure

```text
├── .env
├── .gitignore
├── requirements.txt
├── config.py
├── ddl.py
├── ingest_transactions.py
├── transform_summary.py
├── run_pipeline.py
├── outputs/
│   ├── daily_account_summary_sample.csv
│   └── quarantine_sample.csv
└── README.md

```

---

## 3. Data Dictionary & Schemas

### Bronze Layer (`payments_lakehouse.bronze_transactions`)

* `transaction_id` (STRING, PK): Application identifier matching `TXN-NNNN`.


* `account_id` (STRING): Account identifier matching `ACC-NNNN`.


* `transaction_date` (TIMESTAMP): Strict ISO 8601 UTC timestamp with `T` separator and `Z` suffix.


* `amount` (DECIMAL(18, 4)): Monetary value strictly greater than 0.


* `currency` (STRING): One of `USD`, `EUR`, `GBP`, `CHF`, `JPY`, `AUD`, `CAD`.


* `transaction_type` (STRING): Lowercase direction: `debit` or `credit`.


* `merchant_name` (STRING): Non-empty and not whitespace-only.


* `merchant_category` (STRING): Permitted transaction category enum.


* `status` (STRING): Lowercase state: `completed`, `pending`, `failed`, `reversed`.


* `country_code` (STRING): Officially assigned ISO 3166-1 alpha-2 country code.


* `_ingestion_timestamp` (TIMESTAMP): UTC ingestion timestamp audit column.


* `_natural_key` (STRING): Delimited concatenation of all fields except `transaction_id` for duplicate checks.



### Quarantine Layer (`payments_lakehouse.quarantine_transactions`)

* `raw_payload` (STRING): Full unedited JSON record received from the upstream API.


* `error_reason` (STRING): Specific schema violation description or duplicate natural key collision reason.


* `_ingestion_timestamp` (TIMESTAMP): UTC timestamp when the record was rejected.



### Gold Mart Layer (`payments_lakehouse.daily_account_summary`)

* `account_id` (STRING, Composite PK): Target customer account.


* `transaction_date` (DATE, Composite PK): Calendar activity date.


* `total_debit_amount` (DECIMAL(18, 4)): Sum of completed debits.


* `total_credit_amount` (DECIMAL(18, 4)): Sum of completed credits.


* `net_amount` (DECIMAL(18, 4)): Financial net position calculated as `total_credit_amount - total_debit_amount`.


* `transaction_count` (BIGINT): Total count of completed events.


* `distinct_merchants` (BIGINT): Count of distinct merchants transacted with.


* `top_category` (STRING): Debit merchant category with the highest total spend on that day.


* `currencies` (STRING): Comma-separated list of transaction currencies recorded.


* `updated_at` (TIMESTAMP): UTC timestamp when the summary record was computed.



---

## 4. Tech Stack & Engineering Rationale

| Component | Technology | Rationale |
| --- | --- | --- |
| **Compute Engine** | **Databricks Serverless** | Rapid provisioning and automated scaling without managing classic cluster VMs. |
| **Storage Engine** | **Delta Lake** | ACID transaction compliance, time travel, schema enforcement, and native idempotent `MERGE` statements.

 |
| **Data Validation** | **Pydantic v2** | High-throughput schema enforcement, regex format validation, and custom enum checks.

 |
| **Standards Enforcement** | **`pycountry`** | Official ISO 3166-1 alpha-2 registry validation, preventing synthetic or non-existent 2-letter codes.

 |
| **Transformation** | **Spark SQL** | Vectorized query processing utilizing analytical window functions for category spend ranking.

 |
| **Configuration** | **`python-dotenv` & Widgets** | Completely decouples credentials and endpoints from application source code.

 |

---

## 5. Automated Data Quality Assertions

The pipeline enforces three automated assertion gates inside `transform_summary.py`:

1. **Primary Key Uniqueness**: Confirms `(account_id, transaction_date)` is strictly unique across all rows in `daily_account_summary`.


2. **Financial Invariant Validation**: Validates that `net_amount == total_credit_amount - total_debit_amount` holds true across all rows.


3. **Reconciliation & Scope Check**: Performs a full outer join between Gold mart metrics and Bronze completed records, guaranteeing that no non-completed transactions are included and that transaction counts match upstream completed source events.



---

## 6. Execution Instructions (Databricks Serverless)

1. Ensure `.env` is configured with valid credentials (or set via Databricks job widgets):


```ini
ASSESSMENT_API_BASE_URL=<API Base URL>
ASSESSMENT_API_KEY=<API Key>
ASSESSMENT_AUTH_TOKEN=<API Token>

```


2. Run `run_pipeline.py` using Databricks Serverless Compute.
3. The pipeline will automatically install runtime dependencies, initialize Delta tables, ingest and quarantine records, perform the Gold transformation, run the assertion tests, and generate CSV artifacts in the `outputs/` folder.



---
