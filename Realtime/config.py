from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

#  Open-Meteo 
AQI_API_URL = os.getenv("AQI_API_URL", "https://air-quality-api.open-meteo.com/v1/air-quality")
AQI_FIELDS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"
TIMEZONE = os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh")
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", 3))

# Locations 
LOCATIONS_DIR = BASE_DIR / "data" / "locations"
SNAPSHOT_DIR  = BASE_DIR / "data" / "snapshots"

# Kafka 
KAFKA_BOOTSTRAP_SERVERS = f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}"
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "aqi-raw")

# Hadoop HDFS
_hdfs_host      = os.getenv("HDFS_HOST", "localhost")
_hdfs_port      = os.getenv("HDFS_PORT", "9000")
HDFS_NAMENODE   = f"hdfs://{_hdfs_host}:{_hdfs_port}"
HDFS_RAW_PATH   = f"{HDFS_NAMENODE}/aqi/raw"
HDFS_CHECKPOINT = f"{HDFS_NAMENODE}/aqi/checkpoints"

# SQL Server 
_ss_host        = os.getenv("SQLSERVER_HOST", "localhost")
_ss_port        = os.getenv("SQLSERVER_PORT", "1433")
_ss_db          = os.getenv("SQLSERVER_DB",   "aqi_db")
SQLSERVER_URL   = (
    f"jdbc:sqlserver://{_ss_host}:{_ss_port};"
    f"databaseName={_ss_db};"
    f"encrypt=false;trustServerCertificate=true;"
)
SQLSERVER_PROPS = {
    "user":     os.getenv("SQLSERVER_USER", "sa"),
    "password": os.getenv("SQLSERVER_PASS", ""),
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# MySQL (test local) 
_my_host        = os.getenv("MYSQL_HOST", "localhost")
_my_port        = os.getenv("MYSQL_PORT", "3307")
_my_db          = os.getenv("MYSQL_DB",   "aqi_db")
MYSQL_URL       = f"jdbc:mysql://{_my_host}:{_my_port}/{_my_db}"
MYSQL_PROPS     = {
    "user":     os.getenv("MYSQL_USER", "aqi_user"),
    "password": os.getenv("MYSQL_PASS", "aqi_pass"),
    "driver":   "com.mysql.cj.jdbc.Driver",
}

# Spark 
SPARK_MASTER             = f"spark://{os.getenv('SPARK_HOST', 'localhost')}:{os.getenv('SPARK_PORT', '7077')}"
SPARK_DRIVER_MEMORY      = os.getenv("SPARK_DRIVER_MEMORY",   "2g")
SPARK_EXECUTOR_MEMORY    = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_SHUFFLE_PARTITIONS = "3"

# Trigger interval 
TRIGGER_HDFS_SECONDS = 60
TRIGGER_SQL_SECONDS  = 30