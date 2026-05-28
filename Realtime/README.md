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
```

Điền **đầy đủ** các biến trong `.env`. Giá trị Postgres / Kafka / HDFS phải **khớp** với `docker-compose.yml` (và ngược lại nếu bạn đổi mật khẩu).

| Nhóm biến | Ghi chú |
|-----------|---------|
| `POSTGRES_*` | User, password, DB — dùng cho Python, `psql`, Superset |
| `KAFKA_*` | Bootstrap cho producer / Spark |
| `HDFS_*` | Namenode cho Spark (`hdfs://host:port`) |
| `TIMEZONE` | Khuyến nghị `Asia/Ho_Chi_Minh` |

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install tzdata
```

> Trên Windows cần `tzdata` cho múi giờ `Asia/Ho_Chi_Minh`.

### 2. Chỉnh đường dẫn Spark/Java (bắt buộc sau khi clone)

Mở `processing/spark_aqi.py`, sửa cho khớp máy bạn:

- `JAVA_HOME`
- `HADOOP_HOME` / `hadoop.home.dir`
- `SPARK_HOME`
- `CHECKPOINT_DIR` → trỏ tới `.../Realtime/data/checkpoints/streaming`

### 3. Khởi động Docker

```powershell
docker compose up -d
```

Đợi Kafka, HDFS (namenode/datanode), Postgres, Redis, Superset sẵn sàng.

**Tạo schema PostgreSQL (lần đầu)** — thay `<user>` và `<db>` bằng `POSTGRES_USER` / `POSTGRES_DB` trong `.env`:

```powershell
Get-Content serving\schema.sql | docker exec -i postgres psql -U <user> -d <db>
```

**Superset:** http://localhost:8088  

- Tài khoản admin: do bước `superset-init` trong `docker-compose.yml` (hoặc `scripts/start_superset.ps1`) — **không ghi trong repo**.
- Kết nối dataset AQI: host `postgres` (trong Docker network), database / user / password lấy từ `.env` (`POSTGRES_*`).
- Bảng dashboard: `aqi_readings`, cột thời gian `event_time`.

### 4. Kiểm tra HDFS

- NameNode UI: http://localhost:9870  
- Đường dữ liệu thô: `hdfs://<HDFS_HOST>:<HDFS_PORT>/aqi/raw` (theo `.env`)

Nếu Spark không ghi được HDFS, xem `hadoop.env` (hostname datanode).

---

## Chạy pipeline

Mở **2 terminal** (thư mục `Realtime`), thứ tự:

| # | Lệnh | Vai trò |
|---|------|---------|
| 1 | `python processing/spark_aqi.py` | Spark Streaming — **chạy trước**, luôn bật |
| 2 | `python collect/scheduler.py` | Fetch Open-Meteo mỗi đầu giờ → Kafka |
| *(tùy chọn)* | `python collect/fetcher.py` | Fetch một lần (test) |

Kiểm tra dữ liệu (dùng user/db từ `.env`):

```powershell
docker exec -i postgres psql -U <user> -d <db> -c "SELECT COUNT(*), MAX(event_time) FROM aqi_readings;"
```

---

## Dừng hệ thống

- `Ctrl+C` trên `spark_aqi.py` và `scheduler.py`
- `docker compose down` (giữ volume)
- `docker compose down -v` — **xóa** dữ liệu Postgres/HDFS trong Docker

---

## Scripts hỗ trợ (`scripts/`)

| Script | Khi nào dùng |
|--------|----------------|
| `reset_checkpoints.ps1` | Spark lỗi offset / đổi topic Kafka |
| `reset_kafka_topic.ps1` | Topic `aqi-raw` mất hoặc sai partition |
| `start_superset.ps1` | Khởi tạo metadata DB + Superset |
| `migrate_timezone.sql` | DB cũ dùng `TIMESTAMPTZ` (một lần) |




## Xử lý sự cố nhanh

| Triệu chứng | Cách xử lý |
|-------------|------------|
| Spark không đọc Kafka | Chạy Spark **trước** scheduler; topic `aqi-raw` (3 partitions) |
| Lỗi offset / checkpoint | `.\scripts\reset_checkpoints.ps1` → restart `spark_aqi.py` |
| `event_time` lệch giờ | `migrate_timezone.sql` hoặc DB mới từ `schema.sql` |
| Fetch lỗi timezone Windows | `pip install tzdata` |
| Superset trống | Dataset `aqi_readings`; Spark + scheduler đang chạy; kiểm tra kết nối DB trong `.env` |

