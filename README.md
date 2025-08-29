<!-- ========== BLOCK 1: INTRO & ARCHITECTURE (paste as-is) ========== -->

# Oracle → Snowflake ETL (AdTech Demo)

This project demonstrates a realistic **DSP/AdTech** data pipeline:
**Oracle** acts as the **source/staging DB**, data is transformed and loaded into **Snowflake**, and **curated tables** power reporting/analytics.

### Why this project?
- Mirrors real-world flow used by DSPs (e.g., Quantcast-like setups).
- Shows end-to-end skills: data generation → Oracle load → ETL → Snowflake staging/curated → analytics.

### High-level Architecture



(Synthetic Ad Events CSV)
│
▼
Oracle (Staging)
│ Extract via Python
▼
Snowflake (Staging)
│ Transform SQL
▼
Snowflake (Production/Curated)
│
▼
BI / SQL Analysis


### Dataset
This repo uses a **synthetic AdTech dataset** (campaign/adset/creative daily metrics).
Columns: `campaign_id, adset_id, creative_id, date, impressions, clicks, spend, conversions`.

> No proprietary/company data is used. This is purely for learning.

<!-- ========== END BLOCK 1 ========== -->

<!-- ========== BLOCK 2: PREREQUISITES & STRUCTURE ========== -->

## Prerequisites

- Ubuntu/Linux (tested on Ubuntu)
- Docker
- Python 3.9+
- Jupyter Notebook
- A Snowflake trial account (for warehouse)

## Recommended Repo Structure



oracle-to-snowflake-etl/
├─ README.md
├─ .gitignore
├─ .env # not committed
├─ data/
│ └─ ads_data_YYYY-MM-DDA.csv # generated CSV with suffix
├─ analytics_reports/
│ └─ analytics_report.ipynb
└─ scripts/
├─ generate_ads_data.py
├─ insert_ads_data.py
├─ oracle_to_snowflake.py
└─ transform_staging.py

### .gitignore (important)


.env
pycache/
.ipynb_checkpoints/
data/*.csv


<!-- ========== END BLOCK 2 ========== -->

<!-- ========== BLOCK 3: ORACLE (DOCKER) SETUP ========== -->

## Step 1 — Run Oracle Free (Docker)

```bash
# Install/start Docker if needed
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable --now docker

# Run Oracle Free container
sudo docker run --name oracle-free -p 1521:1521 -d \
  -e ORACLE_PASSWORD=ChangeMe123 \
  -e APP_USER=APP_USER \
  -e APP_USER_PASSWORD=ChangeMe123 \
  gvenzl/oracle-free


Check logs until you see "DATABASE IS READY TO USE!":

sudo docker logs -f oracle-free

Create Oracle table (optional quick check from container)
sudo docker exec -it oracle-free sqlplus APP_USER/ChangeMe123@localhost:1521/FREEPDB1

-- inside SQL*Plus:
CREATE TABLE ADS_DATA (
  CAMPAIGN_ID NUMBER,
  ADSET_ID    NUMBER,
  CREATIVE_ID NUMBER,
  EVENT_DATE  DATE,
  IMPRESSIONS NUMBER,
  CLICKS      NUMBER,
  SPEND       NUMBER(10,2),
  CONVERSIONS NUMBER
);
EXIT;


Connection DSN: localhost:1521/FREEPDB1 (user APP_USER, pass ChangeMe123)

<!-- ========== END BLOCK 3 ========== --> <!-- ========== BLOCK 4: SYNTHETIC DATA GENERATION ========== -->
Step 2 — Generate Synthetic AdTech Dataset

Install Python deps (project root):

pip install pandas numpy jupyter


Create scripts/generate_ads_data.py with:

import pandas as pd
import random
from datetime import datetime, timedelta
import os

num_campaigns = 50
num_adsets_per_campaign = 5
num_creatives_per_adset = 3
num_days = 60

rows = []
start_date = datetime.today() - timedelta(days=num_days)

for campaign_id in range(1, num_campaigns+1):
    for adset_id in range(1, num_adsets_per_campaign+1):
        for creative_id in range(1, num_creatives_per_adset+1):
            for day in range(num_days):
                date = start_date + timedelta(days=day)
                impressions = random.randint(500,5000)
                clicks = random.randint(0,int(impressions*0.2))
                spend = round(random.uniform(10,200),2)
                conversions = random.randint(0, clicks)
                rows.append([
                    campaign_id, adset_id, creative_id,
                    date.strftime("%Y-%m-%d"),
                    impressions, clicks, spend, conversions
                ])

df = pd.DataFrame(rows, columns=[
    "campaign_id","adset_id","creative_id","date",
    "impressions","clicks","spend","conversions"
])

os.makedirs("../data", exist_ok=True)

# CSV filename with suffix logic
base_name = f"ads_data_{datetime.today().strftime('%Y-%m-%d')}"
suffix = "A"
while os.path.exists(f"../data/{base_name}{suffix}.csv"):
    suffix = chr(ord(suffix)+1)
csv_file = f"../data/{base_name}{suffix}.csv"
df.to_csv(csv_file, index=False)
print("Generated:", csv_file, "Rows:", len(df))


Run:

python scripts/generate_ads_data.py

<!-- ========== END BLOCK 4 ========== --> <!-- ========== BLOCK 5: LOAD CSV INTO ORACLE ========== -->
Step 3 — Load CSV into Oracle

Install Oracle driver:

pip install oracledb


insert_ads_data.py (idempotent + duplicate check):

import pandas as pd
import oracledb
import os

data_folder = "../data"
csv_files = sorted([f for f in os.listdir(data_folder) if f.startswith("ads_data_") and f.endswith(".csv")])
latest_csv = os.path.join(data_folder, csv_files[-1])

df = pd.read_csv(latest_csv)
print("Using CSV:", latest_csv, "Rows:", len(df))

conn = oracledb.connect(user="APP_USER", password="ChangeMe123", dsn="localhost:1521/FREEPDB1")
cur = conn.cursor()

# fetch existing keys to avoid duplicates
cur.execute("SELECT CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, EVENT_DATE FROM ADS_DATA")
existing_rows = cur.fetchall()
existing_df = pd.DataFrame(existing_rows, columns=['campaign_id','adset_id','creative_id','date'])
existing_df['date'] = pd.to_datetime(existing_df['date'])
df['date'] = pd.to_datetime(df['date'])

# keep only new rows
merged = df.merge(existing_df, on=['campaign_id','adset_id','creative_id','date'], how='left', indicator=True)
df_new = merged[merged['_merge']=='left_only'].drop(columns=['_merge'])
print("New rows to insert:", len(df_new))

for _, row in df_new.iterrows():
    cur.execute("""
        INSERT INTO ADS_DATA (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, EVENT_DATE, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
        VALUES (:1,:2,:3,TO_DATE(:4,'YYYY-MM-DD'),:5,:6,:7,:8)
    """, (int(row['campaign_id']), int(row['adset_id']), int(row['creative_id']),
          row['date'].strftime('%Y-%m-%d'), int(row['impressions']),
          int(row['clicks']), float(row['spend']), int(row['conversions'])))
conn.commit()
cur.close()
conn.close()

<!-- ========== END BLOCK 5 ========== --> <!-- ========== BLOCK 6: SNOWFLAKE CONNECTOR & STAGING LOAD ========== -->
Step 4 — Oracle → Snowflake (Staging)

.env:

ORACLE_USER=APP_USER
ORACLE_PASSWORD=ChangeMe123
ORACLE_DSN=localhost:1521/FREEPDB1

SNOWFLAKE_USER=<user>
SNOWFLAKE_PASSWORD=<pass>
SNOWFLAKE_ACCOUNT=<account>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ORACLE_ETL
SNOWFLAKE_SCHEMA=STAGING


scripts/oracle_to_snowflake.py:

import os, pandas as pd, oracledb, snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=UserWarning)
load_dotenv()

oconn = oracledb.connect(user=os.getenv("ORACLE_USER"),
                         password=os.getenv("ORACLE_PASSWORD"),
                         dsn=os.getenv("ORACLE_DSN"))
df = pd.read_sql("SELECT * FROM ADS_DATA", oconn)
oconn.close()

df.rename(columns={'EVENT_DATE':'DT'}, inplace=True)
df['DT'] = pd.to_datetime(df['DT'])

sconn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cur = sconn.cursor()
cur.execute("""
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

existing_keys_df = pd.read_sql("SELECT CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT FROM ADS_DATA_STG", sconn)
existing_keys_df['DT'] = pd.to_datetime(existing_keys_df['DT'])

df_new = df.merge(existing_keys_df, on=['CAMPAIGN_ID','ADSET_ID','CREATIVE_ID','DT'], how='left', indicator=True)
df_new = df_new[df_new['_merge']=='left_only'].drop(columns=['_merge'])

rows_sf = [(int(r.CAMPAIGN_ID), int(r.ADSET_ID), int(r.CREATIVE_ID),
            r.DT.strftime("%Y-%m-%d"), int(r.IMPRESSIONS), int(r.CLICKS),
            float(r.SPEND), int(r.CONVERSIONS)) for r in df_new.itertuples(index=False)]
if rows_sf:
    cur.executemany("""
        INSERT INTO ADS_DATA_STG
        (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows_sf)
    sconn.commit()
cur.close()
sconn.close()

<!-- ========== END BLOCK 6 ========== --> <!-- ========== BLOCK 7: TRANSFORM STAGING → PRODUCTION ========== -->
Step 5 — Transform Staging → Production (Curated)

scripts/transform_staging.py:

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
sconn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cur = sconn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS PRODUCTION")
cur.execute("""
CREATE OR REPLACE TABLE PRODUCTION.CAMPAIGN_DAILY_METRICS AS
SELECT
    CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS,
    IFF(NULLIF(IMPRESSIONS,0) IS NULL,0,CLICKS/IMPRESSIONS::FLOAT) AS CTR,
    IFF(NULLIF(IMPRESSIONS,0) IS NULL,0,(SPEND/IMPRESSIONS)*1000) AS CPM,
    IFF(NULLIF(CLICKS,0) IS NULL,0,SPEND/CLICKS) AS CPC,
    IFF(NULLIF(CONVERSIONS,0) IS NULL,0,SPEND/CONVERSIONS) AS CPA
FROM STAGING.ADS_DATA_STG
""")
cur.close()
sconn.close()

<!-- ========== END BLOCK 7 ========== --> <!-- ========== BLOCK 8: ANALYTICS & VISUALS ========== -->
Step 6 — Analytics (Python/Matplotlib/Seaborn)

scripts/analytics_report.py:

import os, pandas as pd, snowflake.connector
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

load_dotenv()
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (12,6)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cur = conn.cursor()
def fetch_df(q): cur.execute(q); return pd.DataFrame(cur.fetchall(),columns=[d[0] for d in cur.description])

top_campaigns = fetch_df("""
SELECT CAMPAIGN_ID, SUM(SPEND) AS TOTAL_SPEND
FROM PRODUCTION.CAMPAIGN_DAILY_METRICS
WHERE DT >= DATEADD('day',-14,CURRENT_DATE)
GROUP BY CAMPAIGN_ID
ORDER BY TOTAL_SPEND DESC
LIMIT 10
""")
sns.barplot(data=top_campaigns,x="CAMPAIGN_ID",y="TOTAL_SPEND")
plt.title("Top 10 Campaigns by Spend (Last 14 Days)")
plt.show()
cur.close()
conn.close()

<!-- ========== END BLOCK 8 ========== --> <!-- ========== BLOCK 9: SECURITY & TROUBLESHOOTING ========== -->
Security & Notes

.env is never committed.

Dataset is synthetic for learning only.

Credentials read via environment variables.

Troubleshooting

Docker permissions → use sudo or add user to docker group.

Oracle DSN/connect errors → ensure container is Up.

Snowflake auth → verify account, warehouse, user, password.

Large loads → use write_pandas or Snowflake stage + COPY INTO.

<!-- ========== END BLOCK 9 ========== -->

---

Tum ise **direct copy paste** kar sakte ho README.md me.  
Ye updated hai:  

- Staging → Production schema flow added (`transform_staging.py`)  
- `.env` updated for `ORACLE_ETL` DB and `STAGING` schema  
- Analytics scripts updated to point **Production.CAMPAIGN_DAILY_METRICS**  
