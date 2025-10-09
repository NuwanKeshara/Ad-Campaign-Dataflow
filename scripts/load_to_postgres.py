from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine

# load data from .env
load_dotenv()

# db configurations
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
CSV_PATH = os.getenv("CSV_PATH")


try:

    # Create Postgres connection
    conn = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Load CSV
    df = pd.read_csv(CSV_PATH)

except Exception as e:
    print(f"Error: {e}")

else:
    # --data cleaning & transformations--

    # drop all empty rows
    df.dropna(how='all', inplace=True)

    # set column names to lowercase and replace spaces with underscores
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    
    # set integer null values with 0
    num_cols = ['impressions', 'clicks', 'spent', 'total_conversion', 'approved_conversion']
    df[num_cols] = df[num_cols].fillna(0)

    # remove negative value rows
    for col in num_cols:
        df = df[df[col] >= 0]

    # clean string values
    df['gender'] = df['gender'].astype(str).str.strip().str.lower()

    # clean the age values
    df['age'] = df['age'].astype(str).str.strip()

    # fill null values with NA
    df['interest'] = df['interest'].fillna('NA')

    # load data to Postgres
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
    print(f"✅ Loaded {len(df)} cleaned rows into Postgres table: {TABLE_NAME}")

finally:
    # close the connection
    conn.close()
