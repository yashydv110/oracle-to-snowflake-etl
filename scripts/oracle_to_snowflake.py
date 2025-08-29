import os
import pandas as pd
import oracledb
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

# Load environment variables
load_dotenv()

ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ORACLE_ETL")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "STAGING")      
PRODUCTION_SCHEMA = os.getenv("PRODUCTION_SCHEMA", "PRODUCTION")
PUBLIC_SCHEMA = os.getenv("PUBLIC_SCHEMA", "PUBLIC")  

# Fetch data from Oracle
print("Connecting to Oracle...")
oconn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
cur = oconn.cursor()

cur.execute("SELECT * FROM ADS_DATA")
rows_oracle = cur.fetchall()
columns = [col[0] for col in cur.description]

df = pd.DataFrame(rows_oracle, columns=columns)

# Rename EVENT_DATE → DT (if exists)
if 'EVENT_DATE' in df.columns:
    df = df.rename(columns={'EVENT_DATE': 'DT'})

# Ensure DT is datetime
df['DT'] = pd.to_datetime(df['DT'])

cur.close()
oconn.close()
print("Columns from Oracle:", df.columns.tolist())
print("Fetched rows from Oracle:", len(df))

# Connect to Snowflake
print("Connecting to Snowflake...")
sconn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cur = sconn.cursor()

# Create staging table if not exists
cur.execute(f"""
CREATE TABLE IF NOT EXISTS ADS_DATA_STG (
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

# Incremental load
existing_keys_df = pd.read_sql(
    "SELECT CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT FROM ADS_DATA_STG",
    sconn
)
existing_keys_df['DT'] = pd.to_datetime(existing_keys_df['DT'])

# Keep only new rows
df_new = df.merge(existing_keys_df, on=['CAMPAIGN_ID','ADSET_ID','CREATIVE_ID','DT'], 
                  how='left', indicator=True)
df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])
print("New rows to insert:", len(df_new))

# Prepare and insert new rows
rows_sf = [
    (
        int(r.CAMPAIGN_ID),
        int(r.ADSET_ID),
        int(r.CREATIVE_ID),
        r.DT.strftime("%Y-%m-%d"),
        int(r.IMPRESSIONS),
        int(r.CLICKS),
        float(r.SPEND),
        int(r.CONVERSIONS)
    )
    for r in df_new.itertuples(index=False)
]

if rows_sf:
    cur.executemany("""
        INSERT INTO ADS_DATA_STG
        (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows_sf)
    sconn.commit()

print("Inserted rows into Snowflake:", len(rows_sf))

# Close connections
cur.close()
sconn.close()
print("ETL completed successfully!")
