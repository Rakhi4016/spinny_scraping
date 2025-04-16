# spinny_scraper.py
import requests
import pandas as pd
from datetime import datetime

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
    cars = res.json().get("results", [])
    if not cars: break

    for car in cars:
        all_records.append({
            "id": car.get("id", ""),
            "car_name": f"{car.get('make', '')} {car.get('model', '')} {car.get('variant', '')}",
            "make_year": car.get("make_year", ""),
            "price": car.get("price", ""),
            "mileage_in_km": car.get("mileage", ""),
            "fuel_type": car.get("fuel_type", ""),
            "transmission": car.get("transmission", ""),
            "car_number": car.get("rto", ""),
            "no_of_owners": car.get("no_of_owners", ""),
            "location": car.get("hub_short_name", ""),
            "emi_per_month": car.get("emi", ""),
            "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    print(f"✅ Page {page}")
    page += 1

pd.DataFrame(all_records).to_csv("spinny_temp_upload.csv", index=False)
print("📁 CSV created.")
