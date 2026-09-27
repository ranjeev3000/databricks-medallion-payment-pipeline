import re
import json
import logging
import requests
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pydantic import BaseModel, field_validator
import pycountry
from pyspark.sql import SparkSession, functions as F

from config import API_BASE_URL, HEADERS, PAGE_SIZE, LOOKBACK_HOURS, CATALOG_SCHEMA

logger = logging.getLogger(__name__)

# Enums & Schema Constants mandated by document rules
ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"}
ALLOWED_TYPES = {"debit", "credit"}
ALLOWED_CATEGORIES = {
    "e-commerce", "travel", "food_and_beverage", "groceries", "electronics",
    "retail", "entertainment", "health", "transportation", "home_and_garden",
    "payroll", "transfer"
}
ALLOWED_STATUSES = {"completed", "pending", "failed", "reversed"}
VALID_COUNTRIES = {country.alpha_2 for country in pycountry.countries}

class TransactionRecord(BaseModel):
    transaction_id: str
    account_id: str
    transaction_date: str
    amount: Decimal
    currency: str
    transaction_type: str
    merchant_name: str
    merchant_category: str
    status: str
    country_code: str

    @field_validator("transaction_id")
    def v_txn_id(cls, v):
        if not re.match(r"^TXN-\d+$", v): raise ValueError(f"Invalid transaction_id format: {v}")
        return v

    @field_validator("account_id")
    def v_acc_id(cls, v):
        if not re.match(r"^ACC-\d+$", v): raise ValueError(f"Invalid account_id format: {v}")
        return v

    @field_validator("transaction_date")
    def v_date(cls, v):
        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", v):
            raise ValueError(f"Date must be strict ISO 8601 UTC with Z suffix: {v}")
        datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        return v

    @field_validator("amount")
    def v_amount(cls, v):
        if v <= 0: raise ValueError(f"Amount must be strictly > 0, got {v}")
        return v

    @field_validator("currency")
    def v_currency(cls, v):
        if v not in ALLOWED_CURRENCIES: raise ValueError(f"Currency {v} not permitted")
        return v

    @field_validator("transaction_type")
    def v_type(cls, v):
        if v not in ALLOWED_TYPES: raise ValueError(f"transaction_type must be lowercase debit/credit: {v}")
        return v

    @field_validator("merchant_name")
    def v_merchant(cls, v):
        if not v or not v.strip(): raise ValueError("merchant_name cannot be empty or whitespace")
        return v

    @field_validator("merchant_category")
    def v_cat(cls, v):
        if v not in ALLOWED_CATEGORIES: raise ValueError(f"merchant_category {v} not permitted")
        return v

    @field_validator("status")
    def v_status(cls, v):
        if v not in ALLOWED_STATUSES: raise ValueError(f"status must be lowercase completed/pending/failed/reversed: {v}")
        return v

    @field_validator("country_code")
    def v_country(cls, v):
        if v not in VALID_COUNTRIES: raise ValueError(f"Invalid ISO 3166-1 alpha-2 country: {v}")
        return v

def get_retry_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=5, 
        backoff_factor=1.0, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session

def get_high_watermark(spark: SparkSession) -> Optional[str]:
    high_wm = spark.table(f"{CATALOG_SCHEMA}.bronze_transactions") \
                   .select(F.max("transaction_date")).collect()[0][0]
    if high_wm:
        buffered = high_wm - timedelta(hours=LOOKBACK_HOURS)
        return buffered.strftime("%Y-%m-%dT%H:%M:%SZ")
    return None

def fetch_api_records(watermark: Optional[str]) -> List[Dict[str, Any]]:
    session = get_retry_session()
    records = []
    offset = 0

    while True:
        params = {"limit": PAGE_SIZE, "offset": offset, "order": "transaction_date.asc"}
        if watermark:
            params["transaction_date"] = f"gte.{watermark}"

        resp = session.get(f"{API_BASE_URL}/transactions", headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break
        records.extend(batch)
        logger.info(f"Fetched {len(batch)} records (Total: {len(records)})")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return records

def ingest_and_stage(spark: SparkSession):
    watermark = get_high_watermark(spark)
    logger.info(f"High watermark with lookback: {watermark}")
    
    raw_records = fetch_api_records(watermark)
    if not raw_records:
        logger.info("No new records to ingest.")
        return

    # Load existing natural keys from Bronze to guard against cross-run duplicates
    existing_keys = set(
        r[0] for r in spark.table(f"{CATALOG_SCHEMA}.bronze_transactions").select("_natural_key").collect()
    )
    seen_in_batch = set()
    valid_rows, quarantined_rows = [], []
    now_utc = datetime.now(timezone.utc)

    for item in raw_records:
        try:
            model = TransactionRecord(**item)
            natural_key = (
                f"{model.account_id}|{model.transaction_date}|{model.amount}|"
                f"{model.currency}|{model.transaction_type}|{model.merchant_name.strip()}|"
                f"{model.merchant_category}|{model.status}|{model.country_code}"
            )
            if natural_key in seen_in_batch or natural_key in existing_keys:
                quarantined_rows.append((
                    json.dumps(item),
                    f"Duplicate transaction detected (natural key collision): {natural_key}",
                    now_utc
                ))
                continue

            seen_in_batch.add(natural_key)
            valid_rows.append((
                model.transaction_id,
                model.account_id,
                datetime.strptime(model.transaction_date, "%Y-%m-%dT%H:%M:%SZ"),
                model.amount,
                model.currency,
                model.transaction_type,
                model.merchant_name,
                model.merchant_category,
                model.status,
                model.country_code,
                now_utc,
                natural_key
            ))
        except Exception as e:
            quarantined_rows.append((json.dumps(item), str(e), now_utc))

    # Append Quarantined records
    if quarantined_rows:
        df_q = spark.createDataFrame(quarantined_rows, ["raw_payload", "error_reason", "_ingestion_timestamp"])
        df_q.write.format("delta").mode("append").saveAsTable(f"{CATALOG_SCHEMA}.quarantine_transactions")

    # Idempotent MERGE into Bronze table
    if valid_rows:
        schema = spark.table(f"{CATALOG_SCHEMA}.bronze_transactions").schema
        df_valid = spark.createDataFrame(valid_rows, schema=schema)
        df_valid.createOrReplaceTempView("staged_bronze")

        spark.sql(f"""
            MERGE INTO {CATALOG_SCHEMA}.bronze_transactions AS target
            USING staged_bronze AS source
            ON target.transaction_id = source.transaction_id
            WHEN NOT MATCHED THEN INSERT *;
        """)

    logger.info(f"Ingestion committed: {len(valid_rows)} valid rows, {len(quarantined_rows)} quarantined rows.")