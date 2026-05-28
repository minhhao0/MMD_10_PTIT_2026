from pathlib import Path
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ── Open-Meteo ─────────────────────────────────────────────────
AQI_API_URL             = os.getenv("AQI_API_URL", "https://air-quality-api.open-meteo.com/v1/air-quality")
AQI_FIELDS              = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"
TIMEZONE                = os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh")
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", 3))

# Windows thường thiếu IANA tzdata — cần `pip install tzdata` hoặc dùng fallback UTC+7
_VN_TZ_FALLBACK = frozenset({"Asia/Ho_Chi_Minh", "Asia/Saigon"})


@lru_cache
def get_local_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(TIMEZONE)
    except Exception:
        if TIMEZONE in _VN_TZ_FALLBACK:
            return timezone(timedelta(hours=7))
        raise


def now_local() -> datetime:
    return datetime.now(get_local_tz())

# ── Locations ──────────────────────────────────────────────────
LOCATIONS_DIR = BASE_DIR / "data" / "locations"
SNAPSHOT_DIR  = BASE_DIR / "data" / "snapshots"

# ── Kafka ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}"
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "aqi-raw")

# ── Hadoop HDFS ────────────────────────────────────────────────
_hdfs_host      = os.getenv("HDFS_HOST", "localhost")
_hdfs_port      = os.getenv("HDFS_PORT", "9000")
HDFS_NAMENODE   = f"hdfs://{_hdfs_host}:{_hdfs_port}"
HDFS_RAW_PATH   = f"{HDFS_NAMENODE}/aqi/raw"
HDFS_CHECKPOINT = f"{HDFS_NAMENODE}/aqi/checkpoints"

# ── PostgreSQL ─────────────────────────────────────────────────
_pg_host        = os.getenv("POSTGRES_HOST", "localhost")
_pg_port        = os.getenv("POSTGRES_PORT", "5432")
_pg_db          = os.getenv("POSTGRES_DB",   "aqi_db")
POSTGRES_URL    = f"jdbc:postgresql://{_pg_host}:{_pg_port}/{_pg_db}"
POSTGRES_PROPS  = {
    "user":     os.getenv("POSTGRES_USER", "aqi_user"),
    "password": os.getenv("POSTGRES_PASS", "aqi_pass"),
    "driver":   "org.postgresql.Driver",
}

# ── Spark (local, dùng Spark cài sẵn trên máy) ────────────────
SPARK_MASTER             = "local[2]"
SPARK_DRIVER_MEMORY      = os.getenv("SPARK_DRIVER_MEMORY",   "2g")
SPARK_EXECUTOR_MEMORY    = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_SHUFFLE_PARTITIONS = "3"
SPARK_PYTHON_WORKER_MEMORY = "1g" # Hoặc "2g" còn nhiều RAM
SPARK_ARROW_ENABLED        = "true" # Kích hoạt Apache Arrow để truyền dữ liệu siêu tốc

# ── Trigger interval ───────────────────────────────────────────
TRIGGER_HDFS_SECONDS = 60
TRIGGER_SQL_SECONDS  = 30