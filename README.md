Oracle → Snowflake ETL (AdTech Demo)

This project demonstrates a realistic DSP/AdTech data pipeline:
Oracle acts as the source/staging DB, data is transformed and loaded into Snowflake, and curated tables power reporting/analytics.

Why this project?

Mirrors real-world flow used by DSPs (e.g., Quantcast-like setups).

Shows end-to-end skills: data generation → Oracle load → ETL → Snowflake staging/curated → analytics.

High-level Architecture
(Synthetic Ad Events CSV)
           │
           ▼
       Oracle (Staging)
           │  Extract via Python
           ▼
     Snowflake (Staging)
           │  Transform SQL
           ▼
   Snowflake (Production/Curated)
           │
           ▼
      BI / SQL Analysis

Dataset

This repo uses a synthetic AdTech dataset (campaign/adset/creative daily metrics).
Columns: campaign_id, adset_id, creative_id, date, impressions, clicks, spend, conversions.

No proprietary/company data is used. This is purely for learning.

<!-- ========== END BLOCK 1 ========== --> <!-- ========== BLOCK 2: PREREQUISITES & STRUCTURE ========== -->
Prerequisites

Ubuntu/Linux (tested on Ubuntu)

Docker

Python 3.9+

Jupyter Notebook

A Snowflake trial account (for warehouse)

Recommended Repo Structure
oracle-to-snowflake-etl/
├─ README.md
├─ .gitignore
├─ .env                  # not committed
├─ data/
│  └─ ads_data_YYYY-MM-DDA.csv   # generated CSV with suffix
├─ analytics_reports/
│  └─ analytics_report.ipynb
└─ scripts/
   ├─ generate_ads_data.py
   ├─ insert_ads_data.py
   ├─ oracle_to_snowflake.py
   └─ transform_staging.py

.gitignore (important)
.env
__pycache__/
.ipynb_checkpoints/
data/*.csv

<!-- ========== END BLOCK 2 ========== --> <!-- ========== BLOCK 3: ORACLE (DOCKER) SETUP ========== -->
Step 1 — Run Oracle Free (Docker)
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


Run the generator:

python scripts/generate_ads_data.py


✔ Creates CSVs under /data with auto-increment suffix like:
ads_data_2025-08-29A.csv, ads_data_2025-08-29B.csv, etc.

<!-- ========== END BLOCK 4 ========== --> <!-- ========== BLOCK 5: LOAD CSV INTO ORACLE ========== -->
Step 3 — Load CSV into Oracle

Install Oracle driver:

pip install oracledb


Run:

python scripts/insert_ads_data.py


✔ This script:

Picks the latest CSV from /data

Checks existing rows in ADS_DATA (by campaign_id, adset_id, creative_id, date)

Inserts only new rows (idempotent, duplicate-safe)

<!-- ========== END BLOCK 5 ========== --> <!-- ========== BLOCK 6: ENV & SNOWFLAKE CONNECTOR ========== -->
Step 4 — Env Variables & Snowflake Connector

Install deps:

pip install snowflake-connector-python python-dotenv


Create .env (do NOT commit this file):

# Oracle
ORACLE_USER=APP_USER
ORACLE_PASSWORD=ChangeMe123
ORACLE_DSN=localhost:1521/FREEPDB1

# Snowflake
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account  # e.g. abcd-xy123
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ORACLE_ETL
SNOWFLAKE_SCHEMA=STAGING

<!-- ========== END BLOCK 6 ========== --> <!-- ========== BLOCK 7: SNOWFLAKE STAGING LOAD ========== -->
Step 5 — Oracle → Snowflake (Staging)

Run:

python scripts/oracle_to_snowflake.py


✔ This script:

Pulls data from Oracle ADS_DATA

Creates table ADS_DATA_STG in Snowflake (if not exists)

Inserts only new rows (incremental load)

<!-- ========== END BLOCK 7 ========== --> <!-- ========== BLOCK 8: TRANSFORM TO CURATED TABLES ========== -->
Step 6 — Transform Staging → Production

Run:

python scripts/transform_staging.py


✔ Creates curated PRODUCTION.CAMPAIGN_DAILY_METRICS with KPIs:

CTR (Click-through rate)

CPM (Cost per 1000 impressions)

CPC (Cost per click)

CPA (Cost per acquisition)

✔ Also creates PUBLIC.CAMPAIGN_DAILY_METRICS_VIEW for BI/SQL tools.

<!-- ========== END BLOCK 8 ========== --> <!-- ========== BLOCK 9: ANALYTICS QUERIES ========== -->
Step 7 — Analytics & Reporting

Open the Jupyter notebook:

analytics_reports/analytics_report.ipynb


Example: Top 10 campaigns by spend (last 14 days):

SELECT CAMPAIGN_ID, SUM(SPEND) AS TOTAL_SPEND
FROM PRODUCTION.CAMPAIGN_DAILY_METRICS
WHERE DT >= DATEADD('day', -14, CURRENT_DATE)
GROUP BY CAMPAIGN_ID
ORDER BY TOTAL_SPEND DESC
LIMIT 10;


You can plot using Matplotlib/Seaborn directly from Snowflake.

<!-- ========== END BLOCK 9 ========== --> <!-- ========== BLOCK 10: SECURITY & TROUBLESHOOTING ========== -->
Security & Notes

Do NOT commit .env (already in .gitignore).

Dataset is synthetic (safe for learning).

Credentials always loaded from environment variables.

Troubleshooting

Docker permission issue → run with sudo or add user to docker group.

Oracle connect error → ensure container is running, DSN = localhost:1521/FREEPDB1.

Snowflake auth error → verify SNOWFLAKE_ACCOUNT, user, warehouse.

For large loads → prefer write_pandas or Snowflake COPY INTO.

<!-- ========== END BLOCK 10 ========== -->
