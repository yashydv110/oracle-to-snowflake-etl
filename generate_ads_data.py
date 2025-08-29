import pandas as pd
import numpy as np
import random
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

# Save to CSV
df.to_csv("ads_data.csv", index=False)

print("Synthetic dataset generated: ads_data.csv")
print(df.head())
print(f"\nTotal rows: {len(df)}")