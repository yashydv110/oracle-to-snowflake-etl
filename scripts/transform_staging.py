import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection
sconn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),  # staging initially
)
cur = sconn.cursor()

# Ensure production and public schemas exist
cur.execute("CREATE SCHEMA IF NOT EXISTS PRODUCTION")
cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")

# Transform data from staging → production
cur.execute("""
CREATE OR REPLACE TABLE PRODUCTION.CAMPAIGN_DAILY_METRICS AS
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

# Create a public view for reporting
cur.execute("""
CREATE OR REPLACE VIEW PUBLIC.CAMPAIGN_DAILY_METRICS_VIEW AS
SELECT * FROM PRODUCTION.CAMPAIGN_DAILY_METRICS;
""")

# Verify row count in production table
cur.execute("SELECT COUNT(*) FROM PRODUCTION.CAMPAIGN_DAILY_METRICS")
count = cur.fetchone()[0]
print("Rows in production table:", count)

cur.close()
sconn.close()
print("Staging → Production transformation and public view creation completed successfully!")