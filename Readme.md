# Bước 1: Thu thập dữ liệu cảm biến từ OpenAQ bằng API (hiện tại)

## 1. Giới thiệu

Tài liệu này mô tả cách xây dựng một hệ thống thu thập dữ liệu các lần đo từ cảm biến môi trường (ví dụ: PM2.5, PM10, NO2, O3, CO, SO2, nhiệt độ, độ ẩm...) thông qua **OpenAQ API**. Hệ thống sẽ gọi API định kỳ, lưu trữ dữ liệu và phục vụ cho mục đích phân tích hoặc xây dựng pipeline dữ liệu (ETL / Data Engineering / Big Data).

---

## 2. Mục tiêu hệ thống

* Thu thập dữ liệu đo từ các cảm biến môi trường
* Tự động gọi API theo lịch (cron / scheduler)
* Lưu dữ liệu vào database hoặc file
* Hỗ trợ mở rộng cho hệ thống Big Data (Kafka, Spark, Data Lake...)

---

## 3. Kiến trúc tổng quan

```text
        +----------------+
        |   Scheduler    |
        |  (cron / Airflow)
        +--------+-------+
                 |
                 v
        +----------------+
        |  Data Collector |
        |  (Python Script)|
        +--------+-------+
                 |
                 v
        +----------------+
        |   OpenAQ API   |
        +----------------+
                 |
                 v
        +----------------+
        |  Storage Layer |
        | (DB / File / Kafka)
        +----------------+
```

---

## 4. Điều kiện tiên quyết

### 4.1 Cài đặt Python

```bash
python --version
```

Khuyến nghị:

```text
Python >= 3.9
```

### 4.2 Cài đặt thư viện

```bash
pip install requests python-dotenv pandas
```

---

## 5. OpenAQ API cơ bản

### Base URL

```text
https://api.openaq.org/v2
```

### Endpoint phổ biến

| Endpoint      | Mô tả                     |
| ------------- | ------------------------- |
| /locations    | Danh sách vị trí cảm biến |
| /latest       | Dữ liệu đo mới nhất       |
| /measurements | Lịch sử đo                |
| /sensors      | Danh sách cảm biến        |

---

## 6. Ví dụ gọi API lấy dữ liệu measurements

### Request

```http
GET https://api.openaq.org/v2/measurements
```

### Tham số thường dùng

| Parameter | Ý nghĩa                           |
| --------- | --------------------------------- |
| city      | Thành phố                         |
| country   | Quốc gia                          |
| parameter | Loại chất đo (pm25, pm10, no2...) |
| limit     | Số bản ghi                        |
| page      | Trang                             |
| date_from | Thời gian bắt đầu                 |
| date_to   | Thời gian kết thúc                |

Ví dụ:

```text
https://api.openaq.org/v2/measurements?city=Hanoi&parameter=pm25&limit=100
```

---

## 7. Ví dụ response JSON

```json
{
  "results": [
    {
      "location": "Hanoi",
      "parameter": "pm25",
      "value": 35.2,
      "unit": "µg/m³",
      "date": {
        "utc": "2026-03-29T01:00:00Z"
      }
    }
  ]
}
```

---

## 8. Lưu trữ dữ liệu

```python

df.to_csv("data.csv")

```

---

## 9. Thu thập dữ liệu định kỳ

### Sử dụng cron

Mở cron:

```bash
crontab -e
```

Chạy mỗi 5 phút:

```bash
*/5 * * * * python /path/to/collector.py
```

---

## 10. Mở rộng cho hệ thống Big Data

Hệ thống này có thể tích hợp thêm:

* Kafka (stream ingestion)
* Spark (batch processing)
* Airflow (workflow scheduling)
* Data Lake (HDFS / S3 / MinIO)
* Dashboard (Grafana / Superset)

Ví dụ pipeline thực tế:

```text
OpenAQ API
   ↓
Collector Service
   ↓
Kafka
   ↓
Spark Streaming
   ↓
Data Warehouse
   ↓
Dashboard
```

---

## 16. Tóm tắt

Hệ thống thu thập dữ liệu từ OpenAQ API bao gồm:

1. Gọi API
2. Parse dữ liệu JSON
3. Lưu trữ dữ liệu
4. Chạy định kỳ
5. Mở rộng pipeline dữ liệu

Đây là bước đầu trong một hệ thống xử lí dữ liệu
