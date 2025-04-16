import requests
import pandas as pd
from datetime import datetime
import os
import snowflake.connector

# Step 1: Scrape from Spinny API
base_url = "https://api.spinny.com/v3/api/listing/v3/"
headers = {"User-Agent": "Mozilla/5.0"}
page = 1
all_records = []

while True:
    params = {
        "city": "hyderabad", "product_type": "cars", "category": "used",
        "page": page, "show_max_on_assured": "true", "custom_budget_sort": "true",
        "prioritize_filter_listing": "true", "high_intent_required": "true",
        "active_banner": "true", "is_max_certified": "0"
    }

    res = requests.get(base_url, params=params, headers=headers)
    if res.status_code != 200:
        break

    cars = res.json().get("results", [])
    if not cars:
        break

    for car in cars:
        all_records.append({
            "id": car.get("id"),
            "car_name": f"{car.get('make', '')} {car.get('model', '')} {car.get('variant', '')}",
            "make_year": car.get("make_year"),
            "price": car.get("price"),
            "mileage_in_km": car.get("mileage"),
            "fuel_type": car.get("fuel_type"),
            "transmission": car.get("transmission"),
            "car_number": car.get("rto"),
            "no_of_owners": car.get("no_of_owners"),
            "location": car.get("hub_short_name"),
            "emi_per_month": car.get("emi"),
            "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    print(f"page {page} scraped")
    page += 1

# Step 2: Save CSV with today's date
today = datetime.today().strftime('%Y-%m-%d')
filename = f"spinny_data_{today}.csv"
df = pd.DataFrame(all_records)
df.to_csv(filename, index=False)
print(f"✅ CSV saved as: {filename}")

# Step 3: Upload to Snowflake (auto-create DB, schema, stage only)
conn = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    role=os.getenv("SF_ROLE")
)

cursor = conn.cursor()

# Auto-create DB and Schema
cursor.execute("CREATE DATABASE IF NOT EXISTS SPINNY_DB")
cursor.execute("USE DATABASE SPINNY_DB")
cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
cursor.execute("USE SCHEMA bronze")

# Auto-create stage
cursor.execute("CREATE STAGE IF NOT EXISTS used_cars_stage")

# Upload CSV to stage
put_sql = f"PUT file://{filename} @used_cars_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
cursor.execute(put_sql)
print(f"📤 Uploaded {filename} to @SPINNY_DB.bronze.used_cars_stage ✅")

conn.close()
