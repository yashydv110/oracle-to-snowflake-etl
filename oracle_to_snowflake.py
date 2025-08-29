import os
import pandas as pd
import oracledb
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ETL_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "STAGING")

# Fetch data from Oracle
print("Connecting to Oracle...")
oconn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
cur = oconn.cursor()

cur.execute("SELECT * FROM ADS_DATA")
rows_oracle = cur.fetchall()
columns = [col[0] for col in cur.description]

df = pd.DataFrame(rows_oracle, columns=columns)
cur.close()
oconn.close()

print("Columns from Oracle:", df.columns.tolist())
print("Fetched rows from Oracle:", len(df))

# Connect to Snowflake
print("Connecting to Snowflake...")
sconn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT
)
cur = sconn.cursor()

# Create warehouse, DB, schema
cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} "
            "WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE")
cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")

# Use the DB + schema + warehouse
cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
cur.execute(f"USE SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")

# Create staging table
cur.execute("""
CREATE OR REPLACE TABLE ADS_DATA_STG (
    CAMPAIGN_ID NUMBER,
    ADSET_ID NUMBER,
    CREATIVE_ID NUMBER,
    DT DATE,
    IMPRESSIONS NUMBER,
    CLICKS NUMBER,
    SPEND NUMBER(10,2),
    CONVERSIONS NUMBER
)
""")

# Prepare data for Snowflake
rows_sf = [
    (
        int(r[0]),  # CAMPAIGN_ID
        int(r[1]),  # ADSET_ID
        int(r[2]),  # CREATIVE_ID
        r[3].strftime("%Y-%m-%d") if hasattr(r[3], 'strftime') else str(r[3]),  # DT
        int(r[4]),  # IMPRESSIONS
        int(r[5]),  # CLICKS
        float(r[6]),# SPEND
        int(r[7])   # CONVERSIONS
    )
    for r in df.itertuples(index=False, name=None)
]

# Load data into Snowflake
cur.execute("TRUNCATE TABLE ADS_DATA_STG")
cur.executemany("""
    INSERT INTO ADS_DATA_STG
    (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""", rows_sf)
sconn.commit()

print("Loaded data into Snowflake staging:", len(rows_sf), "rows")

# Close Snowflake connection
cur.close()
sconn.close()
print("ETL completed successfully!")
