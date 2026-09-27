import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Resolve project base directory reliably in Databricks interactive sessions
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

# Load variables from .env into os.environ
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_FILE_PATH)

def resolve_param(widget_name: str, env_var: str) -> str:
    """
    Priority:
    1. Databricks Widget parameter (if passed by workflow/job)
    2. Environment variable (.env or OS environment)
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        val = dbutils.widgets.get(widget_name)
        if val and val.strip():
            return val.strip()
    except Exception:
        pass

    env_val = os.getenv(env_var)
    if env_val and env_val.strip():
        return env_val.strip()

    raise ValueError(f"Missing required configuration for '{env_var}' in .env or environment.")

# Ingest settings loaded from .env
API_BASE_URL = resolve_param("api_base_url", "ASSESSMENT_API_BASE_URL")
API_KEY = resolve_param("api_key", "ASSESSMENT_API_KEY")
AUTH_TOKEN = resolve_param("auth_token", "ASSESSMENT_AUTH_TOKEN")

# Supabase PostgREST header schema
HEADERS = {
    "apikey": API_KEY,
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

PAGE_SIZE = 100
LOOKBACK_HOURS = 1
CATALOG_SCHEMA = "payments_lakehouse"
VOLUME_OUTPUT_PATH = f"/Volumes/{CATALOG_SCHEMA}/assessment_outputs"