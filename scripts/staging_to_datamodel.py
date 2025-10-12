import os
import sys
import logging
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/staging_to_datamodel.log"),
        logging.StreamHandler(sys.stdout)
        ]
)


# load environment
load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_STG_TABLE = os.getenv("BQ_STG_TABLE")
BQ_DM_TABLE = os.getenv("BQ_DM_TABLE")



# create BigQuery client
def get_bq_client():
    try:
        client = bigquery.Client(project=BQ_PROJECT)
        logging.info(f"Connected to BigQuery project: {BQ_PROJECT}")
        return client
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}")
        sys.exit(1)



# read data from staging table
def read_staging_data(client: bigquery.Client) -> pd.DataFrame:
    query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_STG_TABLE}`"
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Loaded {len(df)} rows from {BQ_STG_TABLE}.")
        return df
    except Exception as e:
        logging.error(f"Error reading from staging table: {e}")
        sys.exit(1)



# transform data and aggregate
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()

        # rename xyz_campaign_id to campaign_id
        df.rename(columns={"xyz_campaign_id": "campaign_id"}, inplace=True)

        # map gender values
        df["gender"] = df["gender"].map({"m": "Male", "f": "Female", "o": "Other"}).fillna("Other")

        # derive age_mid (mean of age range)
        df["age_mid"] = (
            df["age"].str.split("-", expand=True)[0].astype(float)
            + df["age"].str.split("-", expand=True)[1].astype(float)
        ) / 2

        # compute derived metrics
        df["click_rate"] = (df["clicks"] / df["impressions"].replace(0, pd.NA)).fillna(0) * 100
        df["spent_rate"] = (df["spent"] / df["clicks"].replace(0, pd.NA)).fillna(0) * 100
        df["reach"] = (df["clicks"] / df["impressions"].replace(0, pd.NA)).fillna(0)

        logging.info("Transformations applied successfully.")
        return df

    except Exception as e:
        logging.error(f"Transformation error: {e}")
        sys.exit(1)



# write data to BigQuery
def write_to_bigquery(df: pd.DataFrame, client: bigquery.Client):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_DM_TABLE}"
    try:
        df.to_gbq(
            destination_table=table_id,
            project_id=BQ_PROJECT,
            if_exists="replace"
        )
        logging.info(f"Refined table '{BQ_DM_TABLE}' created successfully in BigQuery.")
    except Exception as e:
        logging.error(f"Error writing refined table: {e}")
        sys.exit(1)



def main():
    client = get_bq_client()
    df = read_staging_data(client)
    transformed_df = transform_data(df)
    write_to_bigquery(transformed_df, client)
    logging.info("Data transformation pipeline completed successfully.")



if __name__ == "__main__":
    main()
