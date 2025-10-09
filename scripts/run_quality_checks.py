import logging
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/quality_checks.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


# load environment variables
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")


# create connection
try:
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    conn = engine.connect()
    logging.info("Connected to Postgres database successfully.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")

    if conn:
        conn.close()

    sys.exit(1)


# check for null values in a column
def check_null_values(table_name: str, column_name: str):
    query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL;")
    result = conn.execute(query).scalar()
    if result > 0:
        logging.warning(f"Found {result} NULL values in {table_name}.{column_name}")
        return False
    logging.info(f"No NULL values in {table_name}.{column_name}")
    return True

# check for duplicate rows based on unique key columns
def check_duplicate_rows(table_name: str, unique_cols: list):
    cols = ', '.join(unique_cols)
    query = text(f"""
        SELECT COUNT(*) FROM (
            SELECT {cols}, COUNT(*) 
            FROM {table_name} 
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        ) AS dup;
    """)
    result = conn.execute(query).scalar()
    if result > 0:
        logging.warning(f"Found {result} duplicate rows in {table_name} based on columns: {cols}")
        return False
    logging.info(f"No duplicate rows in {table_name} based on {cols}")
    return True

# check numeric columns have non negative values
def check_valid_ranges(table_name: str, column_name: str, min_val: float = 0.0):
    query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} < {min_val};")
    result = conn.execute(query).scalar()
    if result > 0:
        logging.warning(f"Found {result} invalid (negative) values in {table_name}.{column_name}")
        return False
    logging.info(f"All {table_name}.{column_name} values are >= {min_val}")
    return True


# execute checks
def run_all_checks():
    logging.info("Starting data quality checks...")

    all_passed = True

    all_passed &= check_null_values(TABLE_NAME, "ad_id")
    all_passed &= check_null_values(TABLE_NAME, "Impressions")
    all_passed &= check_null_values(TABLE_NAME, "Clicks")
    all_passed &= check_duplicate_rows(TABLE_NAME, ["ad_id", "xyz_campaign_id", "fb_campaign_id"])
    all_passed &= check_valid_ranges(TABLE_NAME, "Impressions", 0)
    all_passed &= check_valid_ranges(TABLE_NAME, "Clicks", 0)
    all_passed &= check_valid_ranges(TABLE_NAME, "Spent", 0)

    if all_passed:
        logging.info("All quality checks PASSED.")
        return 0
    else:
        logging.error("One or more data quality checks FAILED.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_checks()
    conn.close()
    sys.exit(exit_code)
