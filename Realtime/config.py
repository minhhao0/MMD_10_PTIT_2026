from pathlib import Path

BASE_DIR = Path(__file__).parent

# Locations
LOCATIONS_DIR = BASE_DIR / "data" / "locations"
SNAPSHOT_DIR  = BASE_DIR / "data" / "snapshots"

# Open-Meteo
AQI_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
AQI_FIELDS  = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"
TIMEZONE    = "Asia/Ho_Chi_Minh"
MAX_CONCURRENT_REQUESTS = 3

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "aqi-raw"