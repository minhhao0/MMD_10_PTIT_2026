import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#  CẤU HÌNH - chỉnh IP cho đúng

# Kafka chạy trên máy ảo Ubuntu
KAFKA_BOOTSTRAP = "<IP_MAY_AO>:9092"
KAFKA_TOPIC     = "aqi-raw"

# Hadoop HDFS chạy trên máy ảo Ubuntu
HDFS_NAMENODE   = "hdfs://<IP_MAY_AO>:9000"
HDFS_RAW_PATH   = f"{HDFS_NAMENODE}/aqi/raw"        # lưu data thô
HDFS_PROCESSED  = f"{HDFS_NAMENODE}/aqi/processed"  # lưu data đã xử lý

# SQL Server chạy trên máy thật Windows
SQLSERVER_URL   = (
    "jdbc:sqlserver://<IP_WINDOWS>:1433;"
    "databaseName=aqi_db;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)
SQLSERVER_PROPS = {
    "user":     "sa",
    "password": "<PASSWORD_SQLSERVER>",
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Checkpoint
CHECKPOINT_HDFS = f"{HDFS_NAMENODE}/aqi/checkpoints"
CHECKPOINT_SQL  = f"{HDFS_NAMENODE}/aqi/checkpoints_sql"
#  SCHEMA message Kafka
SCHEMA = StructType([
    StructField("province",  StringType()),
    StructField("district",  StringType()),
    StructField("region",    StringType()),
    StructField("lat",       DoubleType()),
    StructField("lon",       DoubleType()),
    StructField("timestamp", StringType()),
    StructField("pm2_5",     DoubleType()),
    StructField("pm10",      DoubleType()),
    StructField("o3",        DoubleType()),
    StructField("no2",       DoubleType()),
    StructField("so2",       DoubleType()),
    StructField("co",        DoubleType()),
])
#  TÍNH AQI
def calc_aqi(C, breakpoints):
    if C is None or C < 0:
        return 0
    for (C_low, C_high, AQI_low, AQI_high) in breakpoints:
        if C_low <= C <= C_high:
            return round(
                ((AQI_high - AQI_low) / (C_high - C_low)) * (C - C_low) + AQI_low
            )
    return 500

# Bảng breakpoints US EPA
BP_PM25 = [
    (0.0,   12.0,   0,  50),
    (12.1,  35.4,  51, 100),
    (35.5,  55.4, 101, 150),
    (55.5,  150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 500.4, 301, 500),
]
BP_PM10 = [
    (0,   54,    0,  50),
    (55,  154,  51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 604, 301, 500),
]
BP_O3 = [
    (0,    54,   0,  50),
    (55,   70,  51, 100),
    (71,   85, 101, 150),
    (86,  105, 151, 200),
    (106, 200, 201, 300),
]
BP_NO2 = [
    (0,     53,   0,  50),
    (54,   100,  51, 100),
    (101,  360, 101, 150),
    (361,  649, 151, 200),
    (650,  1249, 201, 300),
    (1250, 2049, 301, 500),
]
BP_SO2 = [
    (0,    35,   0,  50),
    (36,   75,  51, 100),
    (76,  185, 101, 150),
    (186, 304, 151, 200),
    (305, 604, 201, 300),
    (605, 1004, 301, 500),
]
BP_CO = [
    (0.0,  4.4,   0,  50),
    (4.5,  9.4,  51, 100),
    (9.5,  12.4, 101, 150),
    (12.5, 15.4, 151, 200),
    (15.5, 30.4, 201, 300),
    (30.5, 50.4, 301, 500),
]

def aqi_category(aqi):
    if aqi is None or aqi <= 0: return "Unknown"
    if aqi <= 50:  return "Good"
    if aqi <= 100: return "Moderate"
    if aqi <= 150: return "Unhealthy for Sensitive Groups"
    if aqi <= 200: return "Unhealthy"
    if aqi <= 300: return "Very Unhealthy"
    return "Hazardous"

# Đăng ký UDF
calc_aqi_pm25 = udf(lambda c: calc_aqi(c, BP_PM25) if c else 0, IntegerType())
calc_aqi_pm10 = udf(lambda c: calc_aqi(c, BP_PM10) if c else 0, IntegerType())
calc_aqi_o3   = udf(lambda c: calc_aqi(c, BP_O3)   if c else 0, IntegerType())
calc_aqi_no2  = udf(lambda c: calc_aqi(c, BP_NO2)  if c else 0, IntegerType())
calc_aqi_so2  = udf(lambda c: calc_aqi(c, BP_SO2)  if c else 0, IntegerType())
calc_aqi_co   = udf(lambda c: calc_aqi(c, BP_CO)   if c else 0, IntegerType())
aqi_cat_udf   = udf(aqi_category, StringType())

#  SINK 1: GHI RAW DATA VÀO HDFS (Parquet, phân vùng theo ngày)
#  Mục đích: lưu trữ lịch sử, không mất data
def write_to_hdfs(batch_df, epoch_id):
    count = batch_df.count()
    if count == 0:
        return
    # Thêm cột date để partition
    df_with_date = batch_df.withColumn("date", to_date("event_time"))
    df_with_date.write \
        .mode("append") \
        .partitionBy("region", "date") \
        .parquet(HDFS_RAW_PATH)
    print(f"Batch {epoch_id}: ghi {count} records vào HDFS → {HDFS_RAW_PATH}")

#  SINK 2: GHI VÀO SQL SERVER (serving layer cho dashboard)
#  Mục đích: dashboard query nhanh
def write_to_sqlserver(batch_df, epoch_id):
    count = batch_df.count()
    if count == 0:
        return
    batch_df.write \
        .jdbc(
            url=SQLSERVER_URL,
            table="dbo.aqi_readings",
            mode="append",
            properties=SQLSERVER_PROPS
        )
    print(f"Batch {epoch_id}: ghi {count} records vào SQL Server")

def main():
    spark = SparkSession.builder \
        .appName("AQI-Vietnam-Streaming") \
        .master("local[2]") \
        .config("spark.jars.packages",
                # Kafka connector
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                # SQL Server JDBC driver
                "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session started")
    print(f"   Kafka  : {KAFKA_BOOTSTRAP}")
    print(f"   HDFS   : {HDFS_RAW_PATH}")
    print(f"   SQL Server: {SQLSERVER_URL}\n")

    #Đọc từ Kafka
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    #Parse JSON
    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*") \
     .withColumn("event_time", to_timestamp("timestamp")) \
     .filter(col("event_time").isNotNull())

    #Tính AQI từng chất
    enriched = parsed \
        .withColumn("aqi_pm25",    calc_aqi_pm25("pm2_5")) \
        .withColumn("aqi_pm10",    calc_aqi_pm10("pm10")) \
        .withColumn("aqi_o3",      calc_aqi_o3("o3")) \
        .withColumn("aqi_no2",     calc_aqi_no2("no2")) \
        .withColumn("aqi_so2",     calc_aqi_so2("so2")) \
        .withColumn("aqi_co",      calc_aqi_co("co")) \
        .withColumn("aqi_final",
            greatest("aqi_pm25", "aqi_pm10", "aqi_o3",
                     "aqi_no2",  "aqi_so2",  "aqi_co")) \
        .withColumn("aqi_category", aqi_cat_udf("aqi_final")) \
        .withColumn("ingested_at",  current_timestamp()) \
        .drop("timestamp")

    # Sink 1: HDFS (raw + enriched, lưu lịch sử)
    query_hdfs = enriched.writeStream \
        .foreachBatch(write_to_hdfs) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_HDFS) \
        .trigger(processingTime="60 seconds") \
        .start()

    # Sink 2: SQL Server (serving layer)
    query_sql = enriched.writeStream \
        .foreachBatch(write_to_sqlserver) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_SQL) \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Spark Streaming đang chạy...")
    print("HDFS  : lưu raw data mỗi 60 giây")
    print("SQL Server: cập nhật serving layer mỗi 30 giây")
    print("hấn Ctrl+C để dừng\n")

    # Chờ cả 2 query
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()