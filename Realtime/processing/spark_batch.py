import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from config import (
    HDFS_RAW_PATH, HDFS_NAMENODE,
    POSTGRES_URL, POSTGRES_PROPS,
    SPARK_MASTER, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY,
    TIMEZONE,
)

def main():
    spark = SparkSession.builder \
        .appName("VN-AQI-Batch") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.memory",       SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory",     SPARK_EXECUTOR_MEMORY) \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.session.timeZone", TIMEZONE) \
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={TIMEZONE}") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Batch started — đọc từ HDFS...")

    # ── Đọc toàn bộ data từ HDFS ────────────────────────────────
    df = spark.read.parquet(HDFS_RAW_PATH)

    # ── Bảng 1: aqi_latest ──────────────────────────────────────
    # Lấy bản ghi MỚI NHẤT của mỗi quận/huyện
    window_latest = Window \
        .partitionBy("province", "district") \
        .orderBy(col("event_time").desc())

    aqi_latest = df \
        .withColumn("rn", row_number().over(window_latest)) \
        .filter(col("rn") == 1) \
        .drop("rn", "year", "month", "day")

    aqi_latest.write.jdbc(
        url        = POSTGRES_URL,
        table      = "aqi_latest",
        mode       = "overwrite",       # ghi đè toàn bộ
        properties = POSTGRES_PROPS,
    )
    print(f"  ✅ aqi_latest: {aqi_latest.count()} rows → PostgreSQL")

    # ── Bảng 2: aqi_hourly ──────────────────────────────────────
    # Trung bình AQI theo từng giờ, chỉ lấy 7 ngày gần nhất
    seven_days_ago = date_add(current_date(), -7)

    aqi_hourly = df \
        .filter(col("event_time") >= seven_days_ago) \
        .withColumn("hour_bucket",
            date_trunc("hour", col("event_time"))) \
        .groupBy("province", "district", "region", "hour_bucket") \
        .agg(
            avg("aqi_final").cast("int").alias("avg_aqi"),
            max("aqi_final").alias("max_aqi"),
            avg("pm2_5").alias("avg_pm25"),
            avg("pm10").alias("avg_pm10"),
            first("aqi_category").alias("aqi_category"),
            first("lat").alias("lat"),
            first("lon").alias("lon"),
        )

    aqi_hourly.write.jdbc(
        url        = POSTGRES_URL,
        table      = "aqi_hourly",
        mode       = "overwrite",
        properties = POSTGRES_PROPS,
    )
    print(f"  aqi_hourly: {aqi_hourly.count()} rows → PostgreSQL")

    print("\nBatch job hoàn thành!")
    spark.stop()

if __name__ == "__main__":
    main()