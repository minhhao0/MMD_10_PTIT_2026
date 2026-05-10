# Real-time AQI Vietnam — Hướng dẫn triển khai

## Kiến trúc hệ thống

```
Windows (máy thật)              Ubuntu (máy ảo)
──────────────────              ───────────────────────────
                                Kafka Broker
fetcher.py ─── produce ──────→  topic: aqi-raw
                                      │
                                      ↓
                                Spark Streaming
                                  - Parse JSON
                                  - Tính Nowcast
                                  - Tính VN_AQI
                                      │
                          ┌───────────┴───────────┐
                          ↓                       ↓
                        HDFS                  SQL Server
                   (lưu lịch sử)          (serving layer)
                   /aqi/raw/                    │
                                               ↓
                                             Dashboard
```

## Cấu trúc thư mục

```
aqi-vietnam/
├── config/
│   └── config.py          ← Cấu hình IP, password
├── collect/
│   ├── locations.py        ← Đọc 3 file CSV
│   ├── fetcher.py          ← Gọi Open-Meteo API
│   └── producer.py         ← Đẩy vào Kafka
├── processing/
│   ├── vn_aqi.py           ← Công thức VN_AQI (QĐ-TCMT 2019)
│   └── spark_aqi.py        ← Spark Streaming chính
├── serving/
│   └── schema.sql          ← SQL Server schema
├── data/
│   ├── locations/          ← 3 file CSV
│   └── snapshots/          ← JSON backup
├── setup.sh                ← Cài đặt Ubuntu
└── start_services.sh       ← Khởi động services
```

## Bước 1 — Cấu hình IP

Mở `config/config.py`, điền đúng IP:

```
KAFKA_BOOTSTRAP_SERVERS = 
HDFS_NAMENODE           = 
SQLSERVER_HOST          = 
SPARK_MASTER            = 
```

## Bước 2 — Cài đặt trên máy ảo 

```bash
chmod +x setup.sh
./setup.sh
```

## Bước 3 — Tạo schema SQL Server 

```bash
sqlcmd -S localhost -U sa -P <password> -i serving/schema.sql
```

## Bước 4 — Khởi động services 

```bash
chmod +x start_services.sh
./start_services.sh
```

## Bước 5 — Chạy Spark 

```bash
python3 processing/spark_aqi.py
```

## Bước 6 — Chạy Fetcher 

```bash
python collect/fetcher.py
```

## Kiểm tra

```bash
# Xem data trong Kafka
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic aqi-raw --max-messages 3

# Xem data trong HDFS
hdfs dfs -ls /aqi/raw/

# Xem data trong SQL Server
sqlcmd -S localhost -U sa -P <pass> -Q "SELECT TOP 5 * FROM aqi_db.dbo.aqi_latest"
```

## Thang AQI Việt Nam (QĐ-TCMT 2019)

| AQI | Mức | Màu |
|-----|-----|-----|
| 0-50 | Tốt | 🟢 Xanh lá |
| 51-100 | Trung bình | 🟡 Vàng |
| 101-150 | Kém | 🟠 Da cam |
| 151-200 | Xấu | 🔴 Đỏ |
| 201-300 | Rất xấu | 🟣 Tím |
| 301-500 | Nguy hại | 🟤 Nâu đỏ |