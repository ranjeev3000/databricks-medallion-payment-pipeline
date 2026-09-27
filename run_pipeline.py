import subprocess
import sys

# Auto-install external dependencies in Databricks Serverless environment
def install_dependencies():
    packages = ["pycountry", "pydantic","python-dotenv"]
    for pkg in packages:
        try:
            __import__(pkg)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

install_dependencies()

# Adding remaining imports
import os
import logging
from pyspark.sql import SparkSession
from ddl import init_tables
from ingest_transactions import ingest_and_stage
from transform_summary import run_transformation, run_tests
from config import VOLUME_OUTPUT_PATH, CATALOG_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def export_submission_samples(spark: SparkSession):
    # Use os.getcwd() which is always defined in Databricks interactive execution
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        base_dir = os.getcwd()

    output_dir = os.path.join(base_dir, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"Exporting sample CSV outputs to workspace path: {output_dir}...")
    
    gold_df = spark.table(f"{CATALOG_SCHEMA}.daily_account_summary")
    quarantine_df = spark.table(f"{CATALOG_SCHEMA}.quarantine_transactions")

    # Export Gold sample
    gold_csv_path = os.path.join(output_dir, "daily_account_summary_sample.csv")
    gold_df.limit(25).toPandas().to_csv(gold_csv_path, index=False)
    
    # Export Quarantine sample
    quarantine_csv_path = os.path.join(output_dir, "quarantine_sample.csv")
    quarantine_df.limit(25).toPandas().to_csv(quarantine_csv_path, index=False)
    
    logger.info(f"Sample CSV deliverables successfully written to: {output_dir}")

def main():
    spark = SparkSession.builder.getOrCreate()
    logger.info("Starting pipeline execution on Databricks Serverless Compute...")

    try:
        # Step 1: DDL & Schema checks
        init_tables(spark)

        # Step 2: Bronze Ingestion with Data Quality Quarantine
        ingest_and_stage(spark)

        # Step 3: Transformation to Gold Mart
        run_transformation(spark)

        # Step 4: Quality Assertions
        run_tests(spark)

        # Step 5: Export deliverables
        export_submission_samples(spark)

        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()