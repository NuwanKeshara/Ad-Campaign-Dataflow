import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/load_to_bigquery.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


# load environment
load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

CSV_PATH = os.getenv("CSV_PATH")
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


# read the csv data
def load_csv_data(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        logging.info(f"Loaded {len(df)} rows from CSV file: {path}")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        sys.exit(1)


# clean and transform data
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting data cleaning process...")

    # drop all empty rows
    df.dropna(how="all", inplace=True)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # validate numeric columns
    num_cols = ["impressions", "clicks", "spent", "total_conversion", "approved_conversion"]
    for col in num_cols:
        if col not in df.columns:
            logging.warning(f"Column '{col}' not found in dataset — skipping.")
            continue
        # fill null values with 0
        df[col] = df[col].fillna(0)

        # remove negative value rows
        df = df[df[col] >= 0]


    # lower gender column
    if "gender" in df.columns:
        df["gender"] = df["gender"].astype(str).str.strip().str.lower()

    # validate age column values
    if "age" in df.columns:
        df["age"] = df["age"].astype(str).str.strip()

    # fill null values with NA
    if "interest" in df.columns:
        df["interest"] = df["interest"].fillna("NA")

    logging.info(f"Data cleaning completed. Remaining rows: {len(df)}")
    return df



# load data into BigQuery
def load_to_bigquery(df: pd.DataFrame, client: bigquery.Client, dataset: str, table: str):
    try:
        table_id = f"{BQ_PROJECT}.{dataset}.{table}"
        df.to_gbq(table_id, project_id=BQ_PROJECT, if_exists="replace")
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table '{table_id}'.")
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}")
        sys.exit(1)



def main():
    client = get_bq_client()
    df = load_csv_data(CSV_PATH)
    cleaned_df = clean_data(df)
    load_to_bigquery(cleaned_df, client, BQ_DATASET, BQ_STG_TABLE)

    logging.info("Database connection closed. Process completed successfully.")


if __name__ == "__main__":
    main()
