import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/load_to_postgres.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# load environment variables
load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_HOST = os.getenv("DB_HOST", "localhost")
# DB_PORT = os.getenv("DB_PORT", "5432")
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
CSV_PATH = os.getenv("CSV_PATH")


# create db connection
def get_db_connection():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        conn = engine.connect()
        logging.info("Database connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Postgres: {e}")
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


    # validate String columns
    if "gender" in df.columns:
        df["gender"] = df["gender"].astype(str).str.strip().str.lower()

    if "age" in df.columns:
        df["age"] = df["age"].astype(str).str.strip()

    if "interest" in df.columns:
        df["interest"] = df["interest"].fillna("NA")

    logging.info(f"Data cleaning completed. Remaining rows: {len(df)}")
    return df


# load data into db
def load_to_postgres(df: pd.DataFrame, conn, table_name: str):
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logging.info(f"Successfully loaded {len(df)} rows into table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error loading data to Postgres: {e}")
        sys.exit(1)



def main():
    conn = get_db_connection()
    df = load_csv_data(CSV_PATH)
    cleaned_df = clean_data(df)
    load_to_postgres(cleaned_df, conn, TABLE_NAME)
    conn.close()
    logging.info("Database connection closed. Process completed successfully.")


if __name__ == "__main__":
    main()
