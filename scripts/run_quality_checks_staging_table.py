import sys
import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/run_quality_checks_staging_table.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


# load environment
load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_STG_TABLE = os.getenv("BQ_STG_TABLE")



# create BigQuery client
def get_bq_client():
    try:
        client = bigquery.Client(project=BQ_PROJECT)
        logging.info(f"BigQuery client created for project: {BQ_PROJECT}")
        return client
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}")
        sys.exit(1)



# run a query and return the count
def run_bq_query(client, query: str) -> int:
    try:
        result = client.query(query).to_dataframe()
        return int(result.iloc[0, 0])
    except Exception as e:
        logging.error(f"Query failed: {e}\nQuery: {query}")
        sys.exit(1)



# quality check for null columns
def check_null_values(client, dataset: str, table: str, column_name: str):
    query = f"""
        SELECT COUNT(*) 
        FROM `{BQ_PROJECT}.{dataset}.{table}` 
        WHERE {column_name} IS NULL
    """
    count = run_bq_query(client, query)
    if count > 0:
        logging.warning(f"Found {count} NULL values in {table}.{column_name}")
        return False
    logging.info(f"No NULL values in {table}.{column_name}")
    return True



# quality check for duplicate rows
def check_duplicate_rows(client, dataset: str, table: str, unique_cols: list):
    cols = ', '.join(unique_cols)
    query = f"""
        SELECT COUNT(*) 
        FROM (
            SELECT {cols}, COUNT(*) AS cnt
            FROM `{BQ_PROJECT}.{dataset}.{table}`
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        )
    """
    count = run_bq_query(client, query)
    if count > 0:
        logging.warning(f"Found {count} duplicate rows in {table} based on {cols}")
        return False
    logging.info(f"No duplicate rows in {table} based on {cols}")
    return True



# quality check for valid ranges in numeric columns
def check_valid_ranges(client, dataset: str, table: str, column_name: str, min_val: float = 0.0):
    query = f"""
        SELECT COUNT(*) 
        FROM `{BQ_PROJECT}.{dataset}.{table}` 
        WHERE {column_name} < {min_val}
    """
    count = run_bq_query(client, query)
    if count > 0:
        logging.warning(f"Found {count} invalid (negative) values in {table}.{column_name}")
        return False
    logging.info(f"All {table}.{column_name} values are >= {min_val}")
    return True



# run all quality checks
def run_all_checks():
    logging.info("Starting data quality checks...")
    client = get_bq_client()
    all_passed = True

    all_passed &= check_null_values(client, BQ_DATASET, BQ_STG_TABLE, "ad_id")
    all_passed &= check_null_values(client, BQ_DATASET, BQ_STG_TABLE, "impressions")
    all_passed &= check_null_values(client, BQ_DATASET, BQ_STG_TABLE, "clicks")
    all_passed &= check_duplicate_rows(client, BQ_DATASET, BQ_STG_TABLE, ["ad_id", "xyz_campaign_id", "fb_campaign_id"])
    all_passed &= check_valid_ranges(client, BQ_DATASET, BQ_STG_TABLE, "impressions", 0)
    all_passed &= check_valid_ranges(client, BQ_DATASET, BQ_STG_TABLE, "clicks", 0)
    all_passed &= check_valid_ranges(client, BQ_DATASET, BQ_STG_TABLE, "spent", 0)

    if all_passed:
        logging.info("All quality checks PASSED.")
        return 0
    else:
        logging.error("One or more data quality checks FAILED.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_checks()
    sys.exit(exit_code)
