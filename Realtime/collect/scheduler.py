import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import math

# Đảm bảo import được module ở thư mục gốc project
sys.path.append(str(Path(__file__).parent.parent))

from collect.fetcher import fetch_all, save_snapshot
from collect.producer import send_records


def _seconds_to_next_hour(now: datetime) -> int:
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    # Làm tròn LÊN để không bị chạy sớm trước :00 do làm tròn xuống.
    return max(1, int(math.ceil((next_hour - now).total_seconds())))


async def job():
    now_str = datetime.now().strftime("%H:%M %d/%m/%Y")
    print(f"\n[{now_str}] Bat dau fetch data...")

    try:
        records = await fetch_all()
        print(f"  Fetch xong: {len(records)} records")

        save_snapshot(records)

        # Đẩy vào Kafka → Spark Streaming sẽ tự nhận và xử lý
        send_records(records, verbose=False)

    except Exception as e:
        print(f"  LOI: {e}")


async def main():
    print("Scheduler khoi dong...")
    print("  -> Fetch moi dau gio (phut 00)")
    print("  -> Nhan Ctrl+C de dung\n")

    # Chạy ngay 1 lần khi khởi động
    await job()

    while True:
        now = datetime.now()
        seconds = _seconds_to_next_hour(now)
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        next_run = next_hour.strftime("%H:%M")
        print(
            f"\n  Lan tiep theo luc {next_run} "
            f"(con {seconds // 60} phut {seconds % 60} giay)"
        )

        await asyncio.sleep(seconds)
        await job()


if __name__ == "__main__":
    # Windows console hay lỗi Unicode: chạy bằng lệnh
    #   set PYTHONIOENCODING=utf-8
    # trước khi start scheduler.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDa dung scheduler.")

