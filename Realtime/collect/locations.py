import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import csv
from config import LOCATIONS_DIR

REGION_FILES = {
    "north":   "north_of_vietnam.csv",
    "central": "central_of_vietnam.csv",
    "south":   "south_of_vietnam.csv",
}

def load_locations() -> list[dict]:
    all_locations = []

    for region, filename in REGION_FILES.items():
        filepath = LOCATIONS_DIR / filename
        with open(filepath, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_locations.append({
                    "province": row["city"].strip(),
                    "district": row["district"].strip(),
                    "region":   region,
                    "lat":      float(row["lat"]),
                    "lon":      float(row["lon"]),
                })

    print(f"Loaded {len(all_locations)} locations "
          f"({sum(1 for l in all_locations if l['region'] == 'north')} north / "
          f"{sum(1 for l in all_locations if l['region'] == 'central')} central / "
          f"{sum(1 for l in all_locations if l['region'] == 'south')} south)")

    return all_locations


if __name__ == "__main__":
    locs = load_locations()
    for region in ["north", "central", "south"]:
        sample = [l for l in locs if l["region"] == region][:2]
        for s in sample:
            print(f"  {s}")