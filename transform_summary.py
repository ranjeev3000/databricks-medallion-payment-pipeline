import logging
from pyspark.sql import SparkSession
from config import CATALOG_SCHEMA

logger = logging.getLogger(__name__)

def run_transformation(spark: SparkSession):
    logger.info("Executing daily account summary transformation...")

    spark.sql(f"""
        WITH completed_txns AS (
            SELECT 
                account_id,
                CAST(transaction_date AS DATE) AS transaction_date,
                transaction_type,
                amount,
                merchant_name,
                merchant_category,
                currency
            FROM {CATALOG_SCHEMA}.bronze_transactions
            WHERE status = 'completed'
        ),
        category_spend AS (
            SELECT 
                account_id,
                transaction_date,
                merchant_category,
                SUM(amount) AS cat_total,
                ROW_NUMBER() OVER(
                    PARTITION BY account_id, transaction_date 
                    ORDER BY SUM(amount) DESC, merchant_category ASC
                ) as rn
            FROM completed_txns
            WHERE transaction_type = 'debit'
            GROUP BY account_id, transaction_date, merchant_category
        ),
        daily_metrics AS (
            SELECT 
                account_id,
                transaction_date,
                COALESCE(SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END), 0) AS total_debit_amount,
                COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END), 0) AS total_credit_amount,
                COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END), 0) - 
                COALESCE(SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END), 0) AS net_amount,
                COUNT(*) AS transaction_count,
                COUNT(DISTINCT merchant_name) AS distinct_merchants,
                CONCAT_WS(', ', ARRAY_SORT(COLLECT_SET(currency))) AS currencies
            FROM completed_txns
            GROUP BY account_id, transaction_date
        ),
        staged_gold AS (
            SELECT 
                m.account_id,
                m.transaction_date,
                m.total_debit_amount,
                m.total_credit_amount,
                m.net_amount,
                m.transaction_count,
                m.distinct_merchants,
                COALESCE(c.merchant_category, 'none') AS top_category,
                m.currencies,
                CURRENT_TIMESTAMP() AS updated_at
            FROM daily_metrics m
            LEFT JOIN category_spend c 
                ON m.account_id = c.account_id 
                AND m.transaction_date = c.transaction_date 
                AND c.rn = 1
        )
        MERGE INTO {CATALOG_SCHEMA}.daily_account_summary AS target
        USING staged_gold AS source
        ON target.account_id = source.account_id AND target.transaction_date = source.transaction_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
    """)

    logger.info("Transformation merged successfully.")

def run_tests(spark: SparkSession):
    logger.info("Running automated quality assertion tests...")
    gold_df = spark.table(f"{CATALOG_SCHEMA}.daily_account_summary")

    # Test 1: Uniqueness of (account_id, transaction_date)
    total_count = gold_df.count()
    distinct_pk_count = gold_df.dropDuplicates(["account_id", "transaction_date"]).count()
    assert total_count == distinct_pk_count, f"Assertion Failed: Primary key collision ({total_count} != {distinct_pk_count})"

    # Test 2: Invariant: net_amount == credit - debit
    mismatched_net = gold_df.filter("net_amount != (total_credit_amount - total_debit_amount)").count()
    assert mismatched_net == 0, f"Assertion Failed: Net amount invariant failed on {mismatched_net} rows"

    # Test 3: Ensure NO account-date in gold mart is derived SOLELY from non-completed records,
    # and that gold transaction_count matches the count of completed records in bronze.
    metric_mismatch = spark.sql(f"""
        WITH expected_completed AS (
            SELECT 
                account_id,
                CAST(transaction_date AS DATE) AS transaction_date,
                COUNT(*) AS expected_cnt,
                COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END), 0) AS expected_net
            FROM {CATALOG_SCHEMA}.bronze_transactions
            WHERE status = 'completed'
            GROUP BY account_id, CAST(transaction_date AS DATE)
        )
        SELECT count(*) AS discrepancies
        FROM expected_completed e
        FULL OUTER JOIN {CATALOG_SCHEMA}.daily_account_summary g
          ON e.account_id = g.account_id AND e.transaction_date = g.transaction_date
        WHERE e.expected_cnt != g.transaction_count 
           OR e.expected_net != g.net_amount
           OR e.account_id IS NULL 
           OR g.account_id IS NULL;
    """).collect()[0]["discrepancies"]

    assert metric_mismatch == 0, f"Assertion Failed: Gold mart aggregations diverge from completed bronze records ({metric_mismatch} discrepancies found)"

    logger.info("All 3 data quality tests passed successfully.")