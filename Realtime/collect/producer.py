import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import json
from confluent_kafka import Producer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id":         "aqi-producer",
    })

def delivery_report(err, msg):
    """Callback sau khi message được gửi."""
    if err:
        print(f"Gửi thất bại: {err}")
    else:
        print(f"Gửi thành công -> topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

def send_records(records: list[dict], verbose: bool = False):
    producer = create_producer()
    success = 0

    for record in records:
        key     = f"{record['province']}_{record['district']}".encode()
        value   = json.dumps(record, ensure_ascii=False).encode()
        producer.produce(
            topic    = KAFKA_TOPIC,
            key      = key,
            value    = value,
            callback = delivery_report if verbose else None,
        )
        success += 1

        # Flush mỗi 100 records để tránh buffer đầy
        if success % 100 == 0:
            producer.flush()
            print(f"Đã gửi {success}/{len(records)} records...")

    # Flush toàn bộ còn lại
    producer.flush()
    print(f"\nGửi xong {success}/{len(records)} records vào topic '{KAFKA_TOPIC}'")