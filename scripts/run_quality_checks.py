from dotenv import load_dotenv
import os
import psycopg2

# load data from .env
load_dotenv()

# db configurations
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

checks = [
    ("negative_values", "SELECT COUNT(*) FROM raw_ad_logs WHERE \"Impressions\"<0 OR \"Clicks\"<0 OR \"Spent\"<0 OR \"Total_Conversion\"<0 OR \"Approved_Conversion\"<0;"),
    ("clicks_gt_impressions", "SELECT COUNT(*) FROM raw_ad_logs WHERE \"Clicks\">\"Impressions\";"),
    ("approved_gt_total", "SELECT COUNT(*) FROM raw_ad_logs WHERE \"Approved_Conversion\">\"Total_Conversion\";")
]

failed = False
for name, q in checks:
    cur.execute(q)
    cnt = cur.fetchone()[0]
    if cnt > 0:
        print(f"[FAIL] {name}: {cnt} rows")
        failed = True
    else:
        print(f"[OK] {name}: 0 rows")

cur.close()
conn.close()

if failed:
    exit(1)
