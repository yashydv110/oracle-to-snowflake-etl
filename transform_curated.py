import os
import snowflake.connector
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Connect to Snowflake
sconn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = sconn.cursor()

# Create curated schema if not exists
cur.execute("CREATE SCHEMA IF NOT EXISTS CURATED")

# Create curated table with KPIs
cur.execute("""
CREATE OR REPLACE TABLE CURATED.CAMPAIGN_DAILY_METRICS AS
SELECT
    CAMPAIGN_ID,
    ADSET_ID,
    CREATIVE_ID,
    DT,
    IMPRESSIONS,
    CLICKS,
    SPEND,
    CONVERSIONS,
    IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, CLICKS/IMPRESSIONS::FLOAT) AS CTR,
    IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, (SPEND/IMPRESSIONS)*1000) AS CPM,
    IFF(NULLIF(CLICKS,0) IS NULL, 0, SPEND/CLICKS) AS CPC,
    IFF(NULLIF(CONVERSIONS,0) IS NULL, 0, SPEND/CONVERSIONS) AS CPA
FROM STAGING.ADS_DATA_STG;
""")

# verify row count
cur.execute("SELECT COUNT(*) FROM CURATED.CAMPAIGN_DAILY_METRICS")
count = cur.fetchone()[0]
print("Rows in curated table:", count)

cur.close()
sconn.close()
