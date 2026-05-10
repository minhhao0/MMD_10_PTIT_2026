import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import asyncio
import httpx
import json
from datetime import datetime

from config import (
    AQI_API_URL,
    AQI_FIELDS,
    TIMEZONE,
    MAX_CONCURRENT_REQUESTS,
    SNAPSHOT_DIR,
)
from collect.locations import load_locations


def get_current_hour_index(times: list[str]) -> int:
    now_str = datetime.now().strftime("%Y-%m-%dT%H:00")
    if now_str in times:
        return times.index(now_str)
    return max(0, len(times) // 2)


async def fetch_one(
    client: httpx.AsyncClient,
    loc: dict,
    semaphore: asyncio.Semaphore
) -> dict | None:

    params = {
        "latitude":      loc["lat"],
        "longitude":     loc["lon"],
        "hourly":        AQI_FIELDS,
        "timezone":      TIMEZONE,
        "forecast_days": 1,
    }

    max_retries = 5
    retry_delay = 2.0

    for attempt in range(max_retries):
        async with semaphore:
            await asyncio.sleep(0.3)
            try:
                r = await client.get(AQI_API_URL, params=params, timeout=15)

                if r.status_code == 429:
                    wait = retry_delay * (attempt + 1)
                    print(f"  429 {loc['province']}/{loc['district']} - chờ {wait:.0f}s (lần {attempt+1}/{max_retries})")
                    await asyncio.sleep(wait)
                    continue

                r.raise_for_status()
                h = r.json()["hourly"]
                idx = get_current_hour_index(h["time"])

                record = {
                    "province":  loc["province"],
                    "district":  loc["district"],
                    "region":    loc["region"],
                    "lat":       loc["lat"],
                    "lon":       loc["lon"],
                    "timestamp": h["time"][idx],
                    "pm2_5":     h["pm2_5"][idx],
                    "pm10":      h["pm10"][idx],
                    "o3":        h["ozone"][idx],
                    "no2":       h["nitrogen_dioxide"][idx],
                    "so2":       h["sulphur_dioxide"][idx],
                    "co":        h["carbon_monoxide"][idx],
                }

                print(f"   {loc['province']:20s}  {loc['district']:20s}  PM2.5={record['pm2_5']}  PM10={record['pm10']}")
                return record

            except httpx.HTTPStatusError as e:
                print(f"   HTTP {e.response.status_code} {loc['province']}{loc['district']}")
                return None
            except Exception as e:
                wait = retry_delay * (attempt + 1)
                print(f"  ⚠️  {loc['province']}{loc['district']}: {e} - chờ {wait:.0f}s")
                await asyncio.sleep(wait)

    print(f"   Bỏ qua sau {max_retries} lần thử: {loc['province']}{loc['district']}")
    return None


async def fetch_all() -> list[dict]:
    locations = load_locations()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    print(f"\n Bắt đầu fetch {len(locations)} locations...\n")

    async with httpx.AsyncClient() as client:
        tasks = [fetch_one(client, loc, semaphore) for loc in locations]
        results = await asyncio.gather(*tasks)

    records = [r for r in results if r is not None]
    failed  = len(locations) - len(records)

    print(f"\nThành công: {len(records)}  Lỗi: {failed}")
    return records


def save_snapshot(records: list[dict]) -> Path:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    fname = SNAPSHOT_DIR / f"aqi_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Snapshot lưu tại: {fname}")
    return fname


def print_top5(records: list[dict]):
    top = sorted(records, key=lambda x: x["pm2_5"] or 0, reverse=True)[:5]
    print("\nTop 5 PM2.5 cao nhất:")
    for r in top:
        print(f"   {r['province']:20s}  {r['district']:20s}  PM2.5={r['pm2_5']}  PM10={r['pm10']}")


if __name__ == "__main__":
    from collect.producer import send_records
    records = asyncio.run(fetch_all())
    # Lưu snapshot JSON
    save_snapshot(records)
    # Gửi vào Kafka
    print("\nĐang gửi vào Kafka...")
    send_records(records, verbose=False)
    print_top5(records)