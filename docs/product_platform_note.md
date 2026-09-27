# Design Note: Product and Platform Thinking

**Author:** Ranjeev Kumar
 

---

## 1. Product Vision: The Payment Lakehouse as a Data Product

Rather than treating data pipelines as mechanical extract-and-load jobs, this lakehouse platform is architected around **data product thinking**. In modern fintech and consumer applications, raw payment logs are messy, inconsistent, and error-prone. Exposing raw transaction feeds directly to downstream consumers causes duplicated modeling effort, inconsistent metrics, and degraded trust.

This platform delivers two verified, purpose-built data products tailored to distinct internal customer personas:


```

+---------------------------------------------------------------------------------------+
|                                Upstream PostgREST API                                 |
+-------------------------------------------+-------------------------------------------+
│
▼
+-----------------------------------------------+
|     Ingestion & Quarantine Enforcement        |
+-----------------------+-----------------------+
│
+-----------------------+-----------------------+
│                                               │
▼                                               ▼
+---------------------------------------+       +---------------------------------------+
|    quarantine_transactions (DLQ)      |       |      daily_account_summary (Mart)     |
|                                       |       |                                       |
|  Consumer: Operations & Data Eng      |       |  Consumer: PMs & Financial Analysts   |
|  Value: Root-cause upstream errors,   |       |  Value: Verified spend trends,        |
|  monitor vendor SLAs, protect Bronze. |       |  category velocity, trusted metrics.  |
+---------------------------------------+       +---------------------------------------+

```

### Downstream Personas & Use Cases

#### 1. Product Managers (Growth, Lifecycle & Merchant Strategy)
* **Core Problem Solved:** Product managers tracking customer retention and engagement frequently run ad-hoc queries against raw transaction tables. They risk counting failed or reversed payments or miscalculating daily spend velocity due to duplicate rows.
* **Delivered Product Value:** The `daily_account_summary` mart provides clean, daily account-level aggregates with out-of-the-box business dimensions:
  * `total_debit_amount` vs. `total_credit_amount`: Distinguishes actual customer outflow from account refunds/top-ups.
  * `top_category`: Identifies where each account concentrated their debit spend each day using analytical ranking windows.
  * `distinct_merchants`: Serves as a direct proxy for merchant diversity and platform engagement.
* **Trust Guardrail:** Only transactions with `status = 'completed'` are aggregated into this mart. Pending, failed, or reversed transactions are strictly filtered out to prevent reporting distorted engagement figures.

#### 2. Financial & Business Intelligence Analysts
* **Core Problem Solved:** Reconciling ledger records manually across time zones and cleaning out-of-order adjustments often produces discrepancy reports and audit friction.
* **Delivered Product Value:** The mart guarantees financial consistency through mathematical invariant assertions (`net_amount == total_credit_amount - total_debit_amount`) executed on every batch. Financial analysts can build revenue and balance dashboards directly atop this Gold mart with zero preliminary data cleaning.

#### 3. Operations & Upstream Vendor Management Teams
* **Core Problem Solved:** In typical pipelines, malformed or duplicate records either cause pipeline crashes or silently pollute the target tables.
* **Delivered Product Value:** The `quarantine_transactions` dead-letter queue isolates defective payloads while capturing diagnostic metadata (`raw_payload`, `error_reason`, `_ingestion_timestamp`). Operations engineers can inspect rejection trends, identify recurring validation breaches (e.g., non-ISO timestamps or malformed country codes), and raise specific compliance tickets with the upstream payment vendor.

---

## 2. Key Architectural Decisions and Trade-Offs

Every production architecture involves pragmatic trade-offs balancing delivery speed, operational complexity, and system resilience.

| Architectural Decision | Chosen Implementation | Alternative Considered | Trade-Off & Justification |
| :--- | :--- | :--- | :--- |
| **Compute & Runtime** | **Databricks Serverless Compute** | Dedicated / Classic Databricks Clusters | Serverless compute spins up in seconds, auto-terminates with zero idle infrastructure spend, and eliminates cluster sizing overhead. The trade-off is an ephemeral compute environment where state must not rely on local node storage, requiring Unity Catalog volumes and tables for persistence. |
| **Transformation Framework** | **Native Spark SQL Delta `MERGE`** | dbt Core CLI Wrapper | The assessment explicitly accepted native SQL assertions as an alternative to dbt. While dbt provides standardized YAML models, running dbt Core inside Databricks Serverless requires managing Python CLI subprocesses, virtual environments, and credential routing for personal access tokens. Native Spark SQL with analytical window functions (`ROW_NUMBER() OVER (...)`) and automated assertions delivers identical idempotency and quality gates with zero external runtime dependencies. |
| **Natural Key Deduplication** | **In-Memory Hash Set Check** | Spark Distributed Anti-Join | Storing existing `_natural_key` strings in an in-memory Python set provides sub-millisecond lookups during stream parsing. While this approach is fast and clean for assessment-scale datasets, it will exceed driver RAM at billion-row scale. At high scale, this logic would transition to a distributed Delta Lake `MERGE` on a generated hash column or a distributed Bloom filter. |
| **Watermark State & Buffering** | **Delta State with 1-Hour Lookback Buffer** | Strict Timestamp Watermarking (`>= MAX`) | Strict watermarking permanently drops late-arriving records that land in the API out of order due to network partitions or gateway retries. Subtracting a 1-hour buffer intentionally re-extracts an overlapping window. While this requires downstream deduplication, our Delta `MERGE` on `transaction_id` handles this idempotently without risk of duplicate rows. |
| **Schema & Contract Gating** | **Pydantic v2 Models** | PySpark Dynamic Schema Checks | Validating row-by-row at extraction time via Pydantic catches corrupt fields before they enter the Spark JVM. This generates descriptive, row-specific error messages (`error_reason`) in the quarantine table. |

---

## 3. Production Evolution: Moving from Prototype to Enterprise Platform

To scale this platform to a high-volume, multi-tenant enterprise deployment, the following four evolutionary phases should be prioritized:


```

+----------------------------------------------------------------------------------------------------+
|                                    Production Pipeline Evolution                                   |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|    Phase 1: DABs & Multi-Environment CI/CD                                                         |
|    - Declarative Databricks Asset Bundles (`databricks.yml`) deployed via GitHub Actions.          |
|    - Automated static analysis (`ruff`, `mypy`) and mock contract tests (`pytest`).                |
|                                                                                                    |
|    Phase 2: Enterprise Multi-Currency Standardization                                              |
|    - Ingest daily foreign exchange rates (e.g., ECB API) into a Silver FX reference table.         |
|    - Calculate both original currency amounts and normalized USD base values (`net_amount_usd`).    |
|                                                                                                    |
|    Phase 3: Observability & Proactive Alerting                                                     |
|    - PagerDuty/Slack webhooks triggered if quarantine rate exceeds 2% of batch volume.             |
|    - Grafana/Databricks dashboards tracking watermark latency and ingestion SLA health.            |
|                                                                                                    |
|    Phase 4: Delta Live Tables (DLT) Transition                                                     |
|    - Migrate procedural Python to DLT pipelines with declarative SQL expectations.                 |
|    - Use `@dlt.expect_or_quarantine` for automatic lineage tracking and dead-letter routing.       |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+

```

### 1. Orchestration and CI/CD via Databricks Asset Bundles (DABs)
* **Infrastructure as Code:** Package all modules (`config.py`, `ddl.py`, `ingest_transactions.py`, `transform_summary.py`) and job schedules into a Databricks Asset Bundle (`databricks.yml`).
* **CI/CD Automation:** Configure GitHub Actions to trigger on pull requests:
  * Run linting and formatting checks using `ruff` and `black`.
  * Validate type contracts using `mypy`.
  * Run unit test suites with mocked API payloads (`pytest` + `responses`) to test pagination, retry backoffs, and quarantine boundaries before deployment.
  * Deploy bundle assets automatically across isolated `dev`, `staging`, and `prod` Unity Catalog targets.

### 2. Multi-Currency Normalization
* **Current Implementation:** Daily account summaries aggregate transactions within their original currencies and record distinct currency codes in a comma-separated column.
* **Enterprise Improvement:** For global accounts transacting across multiple currencies (`USD`, `EUR`, `GBP`), raw amounts cannot be directly summed into a single financial balance[cite: 1, 2]. The platform should ingest daily foreign exchange rates into a Silver currency dimension table and output normalized balances (e.g., `net_amount_usd`, `total_spend_usd`) alongside original currency breakdowns.

### 3. Monitoring, SLA Tracking, and Anomaly Alerting
* **Dead-Letter Spike Alerts:** Configure automated alerts when `COUNT(*)` in `quarantine_transactions` exceeds 2% of the batch volume over a 6-hour rolling window.
* **Data Freshness SLAs:** Emit CloudWatch or Databricks system metrics tracking $(T_{\text{current}} - T_{\text{watermark}})$. If watermark progression lags behind real-time UTC by more than 2 hours, trigger an alert to the on-call data engineering rotation.
* **Audit Lineage:** Use Unity Catalog data lineage to automatically capture column-level dependencies from `bronze_transactions` through to `daily_account_summary`.

### 4. Transition to Delta Live Tables (DLT)
* For teams seeking a fully declarative paradigm, this modular code can be mapped directly into Delta Live Tables:
  * Ingestion and contract enforcement: Built using `@dlt.table` and `@dlt.expect_or_quarantine` rules.
  * Gold transformation: Declared using incremental streaming tables with built-in data quality metrics and automatic UI lineage tracking.

```