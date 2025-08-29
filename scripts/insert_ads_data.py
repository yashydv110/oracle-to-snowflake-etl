import pandas as pd
import oracledb
import os

# Pick latest CSV (optional: based on date + suffix)
data_folder = "../data"
csv_files = sorted([f for f in os.listdir(data_folder) if f.startswith("ads_data_") and f.endswith(".csv")])
latest_csv = os.path.join(data_folder, csv_files[-1])

df = pd.read_csv(latest_csv)
print(f"Using CSV: {latest_csv}")
print(f"Total rows in CSV: {len(df)}")

# Connect to Oracle
connection = oracledb.connect(
    user="APP_USER",
    password="ChangeMe123",
    dsn="localhost:1521/FREEPDB1"
)
cursor = connection.cursor()

# Fetch existing keys from Oracle to avoid duplicates
cursor.execute("SELECT CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, EVENT_DATE FROM ADS_DATA")
existing_rows = cursor.fetchall()
existing_df = pd.DataFrame(existing_rows, columns=['campaign_id','adset_id','creative_id','date'])
existing_df['date'] = pd.to_datetime(existing_df['date'])
df['date'] = pd.to_datetime(df['date'])

# Keep only new rows
merged = df.merge(existing_df, on=['campaign_id','adset_id','creative_id','date'], how='left', indicator=True)
df_new = merged[merged['_merge']=='left_only'].drop(columns=['_merge'])
print(f"New rows to insert: {len(df_new)}")

# Insert new rows only
for _, row in df_new.iterrows():
    cursor.execute("""
        INSERT INTO ADS_DATA (
            CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, EVENT_DATE,
            IMPRESSIONS, CLICKS, SPEND, CONVERSIONS
        ) VALUES (:1, :2, :3, TO_DATE(:4,'YYYY-MM-DD'), :5, :6, :7, :8)
    """, (
        int(row['campaign_id']),
        int(row['adset_id']),
        int(row['creative_id']),
        row['date'].strftime('%Y-%m-%d'),
        int(row['impressions']),
        int(row['clicks']),
        float(row['spend']),
        int(row['conversions'])
    ))

connection.commit()
cursor.close()
connection.close()
print("Data successfully inserted into Oracle ADS_DATA table")