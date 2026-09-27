import logging
from pyspark.sql import SparkSession
from config import CATALOG_SCHEMA

logger = logging.getLogger(__name__)

def init_tables(spark: SparkSession):
    logger.info("Initializing schemas, Delta tables, and UC Volume...")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_SCHEMA};")
    spark.sql(f"USE {CATALOG_SCHEMA};")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_SCHEMA}.assessment_outputs;")

    # 1. Raw Bronze Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.bronze_transactions (
            transaction_id STRING,
            account_id STRING,
            transaction_date TIMESTAMP,
            amount DECIMAL(18, 4),
            currency STRING,
            transaction_type STRING,
            merchant_name STRING,
            merchant_category STRING,
            status STRING,
            country_code STRING,
            _ingestion_timestamp TIMESTAMP,
            _natural_key STRING
        ) USING DELTA;
    """)

    # 2. Dead-Letter / Quarantine Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.quarantine_transactions (
            raw_payload STRING,
            error_reason STRING,
            _ingestion_timestamp TIMESTAMP
        ) USING DELTA;
    """)

    # 3. Gold Aggregation Mart Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.daily_account_summary (
            account_id STRING,
            transaction_date DATE,
            total_debit_amount DECIMAL(18, 4),
            total_credit_amount DECIMAL(18, 4),
            net_amount DECIMAL(18, 4),
            transaction_count BIGINT,
            distinct_merchants BIGINT,
            top_category STRING,
            currencies STRING,
            updated_at TIMESTAMP
        ) USING DELTA;
    """)
    logger.info("Tables and Volume initialized successfully.")