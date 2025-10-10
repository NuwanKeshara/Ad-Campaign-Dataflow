import os
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Load environment variables
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DM_TABLE_NAME = os.getenv("DM_TABLE_NAME")

try:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    logging.info("Connected to Postgres successfully.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    sys.exit(1)


# read data from staging table
try:
    df = pd.read_sql("SELECT * FROM stg_ad_logs", con=engine)
    logging.info(f"Loaded {len(df)} rows from stg_ad_logs.")
except Exception as e:
    logging.error(f"Error reading from staging table: {e}")
    sys.exit(1)


# data transformations and aggregations
try:
    # rename xyz_campaign_id to campaign_id
    df.rename(columns={"xyz_campaign_id": "campaign_id"}, inplace=True)

    # replace gender values with real values
    df["gender"] = df["gender"].map({"m": "Male", "f": "Female", "o":"Other"}).fillna("Other")

    # add new column age_mid as average of age range value
    df["age_mid"] = (
        df["age"].str.split("-", expand=True)[0].astype(float)
        + df["age"].str.split("-", expand=True)[1].astype(float)
    ) / 2

    # click rate = (clicks / impressions) * 100
    df["click_rate"] = (df["clicks"] / df["impressions"].replace(0, pd.NA)).fillna(0) * 100

    # spent rate = (spent / clicks) * 100
    df["spent_rate"] = (df["spent"] / df["clicks"].replace(0, pd.NA)).fillna(0) * 100

    # reach = clicks / impressions
    df["reach"] = (df["clicks"] / df["impressions"].replace(0, pd.NA)).fillna(0)

    logging.info("Transformations applied successfully.")

except Exception as e:
    logging.error(f"Transformation error: {e}")
    sys.exit(1)

# write dm table to postgres
try:
    df.to_sql("dm_ad_logs", con=engine, if_exists="replace", index=False)
    logging.info("Refined table 'dm_ad_logs' created successfully.")
except Exception as e:
    logging.error(f"Error writing refined table: {e}")
finally:
    engine.dispose()
