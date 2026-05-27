# Cấu hình Apache Superset — mount vào container
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "aqi-superset-dev-secret-change-me")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://aqi_user:aqi_pass@postgres:5432/superset_meta",
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60,
    "CACHE_KEY_PREFIX": "aqi_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Dashboard realtime: refresh 10s–5 phút
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [10, "10 giây"],
    [30, "30 giây"],
    [60, "1 phút"],
    [120, "2 phút"],
    [300, "5 phút"],
]

# Múi giờ VN (khớp Spark / fetcher)
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
