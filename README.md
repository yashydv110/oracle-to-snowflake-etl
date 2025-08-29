<!-- ========== BLOCK 1: INTRO & ARCHITECTURE (paste as-is) ========== -->

# Oracle → Snowflake ETL (AdTech Demo)

This project demonstrates a realistic **DSP/AdTech** data pipeline:
**Oracle** acts as the **source/staging DB**, data is transformed and loaded into **Snowflake**, and **curated tables** power reporting/analytics.

### Why this project?
- Mirrors real-world flow used by DSPs (e.g., Quantcast-like setups).
- Shows end-to-end skills: data generation → Oracle load → ETL → Snowflake staging/curated → analytics.

### High-level Architecture

```
(Synthetic Ad Events CSV)
           │
           ▼
       Oracle (Staging)
           │  Extract via Python
           ▼
     Snowflake (Staging)
           │  Transform SQL
           ▼
   Snowflake (Curated/Data Marts)
           │
           ▼
      BI / SQL Analysis
```

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

```
oracle-to-snowflake-etl/
├─ README.md
├─ .gitignore
├─ .env                  # not committed
├─ data/
│  └─ ads_data.csv       # generated 
├─ analytics_reports/
│  └─ analytics_report.ipynb
└─ scripts/
   ├─ generate_ads_data.py
   ├─ insert_ads_data.py
   ├─ oracle_to_snowflake.py
   └─ transform_curated.py
```
## Recommended Repo Structure


### .gitignore (important)
```
.env
__pycache__/
.ipynb_checkpoints/
data/*.csv
```

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
```

Check logs until you see "DATABASE IS READY TO USE!":
```bash
sudo docker logs -f oracle-free
```

### Create Oracle table (optional quick check from container)
```bash
sudo docker exec -it oracle-free sqlplus APP_USER/ChangeMe123@localhost:1521/FREEPDB1

-- inside SQL*Plus:
-- this table will later be bulk-loaded from CSV
CREATE TABLE ADS_DATA (
  CAMPAIGN_ID NUMBER,
  ADSET_ID    NUMBER,
  CREATIVE_ID NUMBER,
  DT          DATE,
  IMPRESSIONS NUMBER,
  CLICKS      NUMBER,
  SPEND       NUMBER(10,2),
  CONVERSIONS NUMBER
);
EXIT;
```

> Connection DSN: `localhost:1521/FREEPDB1` (user `APP_USER`, pass `ChangeMe123`)

<!-- ========== END BLOCK 3 ========== -->

<!-- ========== BLOCK 4: SYNTHETIC DATA GENERATION ========== -->

## Step 2 — Generate Synthetic AdTech Dataset

Install Python deps (project root):
```bash
pip install pandas numpy jupyter
```

Create `scripts/generate_data.py` with:
```python
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path

num_campaigns = 50
num_adsets_per_campaign = 5
num_creatives_per_adset = 3
num_days = 60  # last 60 days

rows = []
start_date = datetime.today() - timedelta(days=num_days)

for campaign_id in range(1, num_campaigns + 1):
    for adset_id in range(1, num_adsets_per_campaign + 1):
        for creative_id in range(1, num_creatives_per_adset + 1):
            for day in range(num_days):
                date = start_date + timedelta(days=day)
                impressions = random.randint(500, 5000)
                clicks = random.randint(0, int(impressions * 0.2))
                spend = round(random.uniform(10, 200), 2)
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

Path("data").mkdir(exist_ok=True)
df.to_csv("data/ads_data.csv", index=False)
print("Generated data/data/ads_data.csv with rows:", len(df))
```

Run the generator:
```bash
python scripts/generate_data.py
```

<!-- ========== END BLOCK 4 ========== -->

<!-- ========== BLOCK 5: LOAD CSV INTO ORACLE ========== -->

## Step 3 — Load CSV into Oracle (from Jupyter or Python)

Install Oracle driver:
```bash
pip install oracledb
```

In `notebooks/etl_pipeline.ipynb` (or a quick script), run:
```python
import pandas as pd
import oracledb

df = pd.read_csv("data/ads_data.csv")

conn = oracledb.connect(user="APP_USER", password="ChangeMe123",
                        dsn="localhost:1521/FREEPDB1")
cur = conn.cursor()

# ensure table exists (idempotent create)
cur.execute("""
BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE ADS_DATA (
    CAMPAIGN_ID NUMBER,
    ADSET_ID    NUMBER,
    CREATIVE_ID NUMBER,
    DT          DATE,
    IMPRESSIONS NUMBER,
    CLICKS      NUMBER,
    SPEND       NUMBER(10,2),
    CONVERSIONS NUMBER
  )';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN RAISE; END IF; -- -955 = name already used
END;
""")

# bulk insert
rows = [
    (int(r.campaign_id), int(r.adset_id), int(r.creative_id),
     r.date, int(r.impressions), int(r.clicks), float(r.spend), int(r.conversions))
    for r in df.itertuples(index=False)
]
cur.executemany("""
  INSERT INTO ADS_DATA
  (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
  VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7, :8)
""", rows)

conn.commit()
print("Loaded rows:", len(rows))

# quick sanity check
cur.execute("SELECT COUNT(*) FROM ADS_DATA")
print("Oracle ADS_DATA count:", cur.fetchone()[0])

cur.close()
conn.close()
```

<!-- ========== END BLOCK 5 ========== -->

<!-- ========== BLOCK 6: ENV & SNOWFLAKE CONNECTOR ========== -->

## Step 4 — Env Variables & Snowflake Connector

Install deps:
```bash
pip install snowflake-connector-python python-dotenv
```

Create `.env` (do NOT commit this file):
```env
# Oracle
ORACLE_USER=APP_USER
ORACLE_PASSWORD=ChangeMe123
ORACLE_DSN=localhost:1521/FREEPDB1

# Snowflake
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account  # e.g. abcd-xy123
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ETL_DB
SNOWFLAKE_SCHEMA=STAGING
```

Add to `.gitignore` (already above):
```
.env
```

<!-- ========== END BLOCK 6 ========== -->

<!-- ========== BLOCK 7: SNOWFLAKE STAGING LOAD ========== -->

## Step 5 — Create Snowflake objects & Load Staging

In Snowflake Worksheet (or via Python), run:
```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE DATABASE IF NOT EXISTS ETL_DB;
CREATE SCHEMA IF NOT EXISTS ETL_DB.STAGING;

-- Staging table (matches Oracle ADS_DATA)
CREATE OR REPLACE TABLE ETL_DB.STAGING.ADS_DATA_STG (
  CAMPAIGN_ID NUMBER,
  ADSET_ID    NUMBER,
  CREATIVE_ID NUMBER,
  DT          DATE,
  IMPRESSIONS NUMBER,
  CLICKS      NUMBER,
  SPEND       NUMBER(10,2),
  CONVERSIONS NUMBER
);
```

Now from Notebook, **extract Oracle → load to Snowflake**:
```python
import os
import pandas as pd
import oracledb, snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Oracle → DataFrame
oconn = oracledb.connect(user=os.getenv("ORACLE_USER"),
                         password=os.getenv("ORACLE_PASSWORD"),
                         dsn=os.getenv("ORACLE_DSN"))
df = pd.read_sql("SELECT * FROM ADS_DATA", oconn)
oconn.close()

# Connect Snowflake
sconn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = sconn.cursor()

# Clear & load
cur.execute("TRUNCATE TABLE IF EXISTS ADS_DATA_STG")
# use executemany for simplicity (for larger data consider write_pandas)
rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
cur.executemany("""
  INSERT INTO ADS_DATA_STG
  (CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS)
  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""", rows)
sconn.commit()

# sanity check
cur.execute("SELECT COUNT(*) FROM ADS_DATA_STG")
print("Snowflake staging count:", cur.fetchone()[0])

cur.close()
sconn.close()
```

> Tip: For very large loads, consider `write_pandas` utility or Snowflake **stages** + **COPY INTO**.

<!-- ========== END BLOCK 7 ========== -->

<!-- ========== BLOCK 8: TRANSFORM TO CURATED TABLES ========== -->

## Step 6 — Transform Staging → Curated (Snowflake SQL)

Create curated table with common KPIs:
```sql
CREATE SCHEMA IF NOT EXISTS ETL_DB.CURATED;

CREATE OR REPLACE TABLE ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS AS
SELECT
  CAMPAIGN_ID,
  ADSET_ID,
  CREATIVE_ID,
  DT,
  IMPRESSIONS,
  CLICKS,
  SPEND,
  CONVERSIONS,
  IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, CLICKS/IMPRESSIONS::FLOAT)    AS CTR,
  IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, (SPEND/IMPRESSIONS)*1000)      AS CPM,
  IFF(NULLIF(CLICKS,0) IS NULL, 0, SPEND/CLICKS)                        AS CPC,
  IFF(NULLIF(CONVERSIONS,0) IS NULL, 0, SPEND/CONVERSIONS)              AS CPA
FROM ETL_DB.STAGING.ADS_DATA_STG;
```

Optional **daily refresh** pattern (truncate+insert):
```sql
TRUNCATE TABLE ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS;
INSERT INTO ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS
SELECT
  CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, DT, IMPRESSIONS, CLICKS, SPEND, CONVERSIONS,
  IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, CLICKS/IMPRESSIONS::FLOAT),
  IFF(NULLIF(IMPRESSIONS,0) IS NULL, 0, (SPEND/IMPRESSIONS)*1000),
  IFF(NULLIF(CLICKS,0) IS NULL, 0, SPEND/CLICKS),
  IFF(NULLIF(CONVERSIONS,0) IS NULL, 0, SPEND/CONVERSIONS)
FROM ETL_DB.STAGING.ADS_DATA_STG;
```

<!-- ========== END BLOCK 8 ========== -->

<!-- ========== BLOCK 9: ANALYTICS QUERIES ========== -->

## Step 7 — Sample Analytics

Top campaigns by spend (last 14 days):
```sql
SELECT CAMPAIGN_ID, SUM(SPEND) AS TOTAL_SPEND
FROM ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS
WHERE DT >= DATEADD('day', -14, CURRENT_DATE)
GROUP BY CAMPAIGN_ID
ORDER BY TOTAL_SPEND DESC
LIMIT 10;
```

Campaign CTR trend:
```sql
SELECT DT, AVG(CTR) AS AVG_CTR
FROM ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS
GROUP BY DT
ORDER BY DT;
```

Creative performance:
```sql
SELECT CREATIVE_ID, SUM(IMPRESSIONS) AS IMPS, SUM(CLICKS) AS CLKS, AVG(CPC) AS AVG_CPC
FROM ETL_DB.CURATED.CAMPAIGN_DAILY_METRICS
GROUP BY CREATIVE_ID
ORDER BY IMPS DESC
LIMIT 10;
```

<!-- ========== END BLOCK 9 ========== -->

<!-- ========== BLOCK 10: SECURITY & TROUBLESHOOTING ========== -->

## Security & Notes
- **Do NOT commit `.env`** (already in `.gitignore`).
- Credentials are read from `.env` via environment variables.
- This is a learning project; dataset is synthetic.

## Troubleshooting
- Docker permission issue → run `sudo docker ...` or add user to `docker` group.
- Oracle connect error → ensure container is Up and DSN = `localhost:1521/FREEPDB1`.
- Snowflake auth error → verify `SNOWFLAKE_ACCOUNT` (e.g., `xy12345.ap-south-1` style), user, warehouse.
- Large loads → prefer `write_pandas` or external stage + `COPY INTO`.

<!-- ========== END BLOCK 10 ========== -->
