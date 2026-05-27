# Realtime AQI Vietnam

Pipeline thu thập chỉ số chất lượng không khí (Open-Meteo) → Kafka → Spark Streaming → HDFS + PostgreSQL → Apache Superset.

```
Open-Meteo API  →  fetcher/scheduler  →  Kafka (aqi-raw)
                                              ↓
                                    Spark Streaming (spark_aqi.py)
                                              ↓
                              HDFS (/aqi/raw) + PostgreSQL (aqi_readings)
                                              ↓
                                    Superset (dashboard)
```

## Yêu cầu

| Thành phần | Phiên bản gợi ý |
|------------|-----------------|
| Windows 10/11 | 64-bit |
| Docker Desktop | Compose v2 |
| Python | 3.11+ |
| JDK | 17 (Eclipse Temurin / Adoptium) |
| Apache Spark | 3.x / 4.x (bin-hadoop3) |
| Hadoop winutils | Cho Spark ghi file local/HDFS client |

## Cài đặt

### 1. Clone và cấu hình môi trường

```powershell
cd Realtime
copy .env.example .env
# Chỉnh .env nếu đổi port/password
```

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install tzdata
```

> Trên Windows cần `tzdata` để dùng múi giờ `Asia/Ho_Chi_Minh`.

### 2. Chỉnh đường dẫn Spark/Java (bắt buộc sau khi clone)

Mở `processing/spark_aqi.py`, sửa cho khớp máy bạn:

- `JAVA_HOME`
- `HADOOP_HOME` / `hadoop.home.dir`
- `SPARK_HOME`
- `CHECKPOINT_DIR` → nên trỏ tới `.../Realtime/data/checkpoints/streaming`

Ví dụ checkpoint:

```python
CHECKPOINT_DIR = "file:///D:/projects/Realtime/data/checkpoints/streaming"
```

### 3. Khởi động Docker

```powershell
docker compose up -d
```

Đợi Kafka, HDFS (namenode/datanode), Postgres, Redis, Superset sẵn sàng.

**Tạo schema PostgreSQL (lần đầu):**

```powershell
Get-Content serving\schema.sql | docker exec -i postgres psql -U aqi_user -d aqi_db
```

**Superset:** http://localhost:8088 — `admin` / `admin`

Kết nối database trong Superset:

- Host: `postgres` (trong Docker network) hoặc `host.docker.internal` / IP máy nếu cấu hình khác
- Database: `aqi_db`
- User: `aqi_user` / `aqi_pass`

Dataset chính: bảng `aqi_readings`, cột thời gian `event_time`.

### 4. Kiểm tra HDFS

- NameNode UI: http://localhost:9870  
- Spark đọc/ghi: `hdfs://localhost:9000/aqi/raw`

Nếu Spark không ghi được HDFS, xem `hadoop.env` (hostname datanode).

---

## Chạy pipeline

Mở **3 terminal** (thư mục `Realtime`), thứ tự:

| # | Lệnh | Vai trò |
|---|------|---------|
| 1 | `python processing/spark_aqi.py` | Spark Streaming — **chạy trước**, luôn bật |
| 2 | `python collect/scheduler.py` | Fetch Open-Meteo mỗi đầu giờ → Kafka |
| 3 | *(tùy chọn)* `python collect/fetcher.py` | Fetch một lần (test) |

Sau vài phút, kiểm tra dữ liệu:

```powershell
docker exec postgres psql -U aqi_user -d aqi_db -c "SELECT COUNT(*), MAX(event_time) FROM aqi_readings;"
```

---

## Dừng hệ thống

- `Ctrl+C` trên `spark_aqi.py` và `scheduler.py`
- `docker compose down` (giữ dữ liệu volume)
- `docker compose down -v` — **xóa toàn bộ** dữ liệu Postgres/HDFS trong Docker

---

## Scripts hỗ trợ (`scripts/`)

| Script | Khi nào dùng |
|--------|----------------|
| `reset_checkpoints.ps1` | Spark lỗi offset / đổi topic Kafka |
| `reset_kafka_topic.ps1` | Topic `aqi-raw` mất hoặc sai số partition |
| `start_superset.ps1` | Khởi tạo `superset_meta` + chạy Superset |
| `migrate_timezone.sql` | DB cũ dùng `TIMESTAMPTZ` (chạy một lần) |

---

## Cấu trúc thư mục

```
Realtime/
├── collect/           # fetcher, scheduler, producer Kafka
├── processing/        # spark_aqi.py (streaming), spark_batch.py (tùy chọn)
├── serving/           # schema.sql, superset_config.py
├── data/
│   ├── locations/     # CSV tỉnh/huyện (commit được)
│   ├── checkpoints/ # Spark — KHÔNG commit
│   └── snapshots/     # JSON backup — KHÔNG commit
├── scripts/
├── docker-compose.yml
├── config.py
├── requirements.txt
└── .env.example
```

---

## File **không** nên push lên GitHub

| Loại | Ví dụ | Lý do |
|------|--------|--------|
| Biến môi trường / mật khẩu | `.env` | Password DB, cấu hình riêng máy |
| Checkpoint Spark | `data/checkpoints/**` | Trạng thái runtime, đường dẫn máy bạn |
| Snapshot JSON | `data/snapshots/*.json` | File lớn, tạo lại khi fetch |
| Cache Python | `__pycache__/`, `.venv/` | Tự sinh |
| Cấu hình IDE | `.vscode/`, `.idea/` | Tuỳ máy từng người |
| Dữ liệu Docker | Volume `postgres_data`, HDFS… | Nằm trên Docker, không trong repo |

**Nên push:** mã nguồn `.py`, `docker-compose.yml`, `hadoop.env`, `serving/schema.sql`, `data/locations/*.csv`, `requirements.txt`, `.env.example`, `.gitignore`, `README.md`, `scripts/*.ps1`.

**Lưu ý:** `processing/spark_aqi.py` có đường dẫn JDK/Spark **của máy dev** — vẫn push được nhưng người clone phải sửa lại (xem mục Cài đặt §2).

---

## Xử lý sự cố nhanh

| Triệu chứng | Cách xử lý |
|-------------|------------|
| Spark không đọc Kafka | Chạy Spark **trước** scheduler; kiểm tra topic `aqi-raw` (3 partitions) |
| Lỗi offset / checkpoint | `.\scripts\reset_checkpoints.ps1` rồi restart `spark_aqi.py` |
| `event_time` lệch 7 giờ | Chạy `migrate_timezone.sql` hoặc DB mới từ `schema.sql` |
| Fetch lỗi timezone Windows | `pip install tzdata` |
| Superset không có data | Dataset trỏ `aqi_readings`; Spark + scheduler đang chạy |

---

## Giấy phép dữ liệu

Dữ liệu khí tượng từ [Open-Meteo](https://open-meteo.com/) / CAMS — cần ghi attribution khi trình bày đồ án.
