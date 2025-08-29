import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# Parameters
num_campaigns = 50
num_adsets_per_campaign = 5
num_creatives_per_adset = 3
num_days = 60   # last 60 days
rows = []

start_date = datetime.today() - timedelta(days=num_days)

# Generate data
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
                    campaign_id,
                    adset_id,
                    creative_id,
                    date.strftime("%Y-%m-%d"),
                    impressions,
                    clicks,
                    spend,
                    conversions
                ])

# Create DataFrame
df = pd.DataFrame(rows, columns=[
    "campaign_id", "adset_id", "creative_id", "date",
    "impressions", "clicks", "spend", "conversions"
])

# Ensure data folder exists
os.makedirs("../data", exist_ok=True)

# CSV filename logic with suffix
base_name = f"ads_data_{datetime.today().strftime('%Y-%m-%d')}"
suffix = "A"
while os.path.exists(f"../data/{base_name}{suffix}.csv"):
    suffix = chr(ord(suffix) + 1)  # next letter
csv_filename = f"../data/{base_name}{suffix}.csv"

# Save CSV
df.to_csv(csv_filename, index=False)
print(f"Synthetic dataset generated: {csv_filename}")
print(df.head())
print(f"\nTotal rows: {len(df)}")