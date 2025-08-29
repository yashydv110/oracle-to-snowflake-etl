import pandas as pd
import oracledb

# Read the generated CSV file
df = pd.read_csv("ads_data.csv")

# Create Oracle connection
connection = oracledb.connect(
    user="APP_USER",
    password="ChangeMe123",
    dsn="localhost:1521/FREEPDB1"
)

cursor = connection.cursor()

# Insert data row by row into Oracle table
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO ADS_DATA (
            CAMPAIGN_ID, ADSET_ID, CREATIVE_ID, EVENT_DATE,
            IMPRESSIONS, CLICKS, SPEND, CONVERSIONS
        ) VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7, :8)
    """, (
        int(row['campaign_id']),
        int(row['adset_id']),
        int(row['creative_id']),
        row['date'],
        int(row['impressions']),
        int(row['clicks']),
        float(row['spend']),
        int(row['conversions'])
    ))

# Commit the transaction and close the connection
connection.commit()
cursor.close()
connection.close()

print("Data successfully inserted into Oracle ADS_DATA table")