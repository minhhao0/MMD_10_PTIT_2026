import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW,
    HDFS_RAW_PATH, HDFS_CHECKPOINT, HDFS_NAMENODE,
    POSTGRES_URL, POSTGRES_PROPS,
    SPARK_MASTER, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY,
    SPARK_SHUFFLE_PARTITIONS,
    TRIGGER_HDFS_SECONDS, TRIGGER_SQL_SECONDS,
)
from processing.vn_aqi import (
    udf_aqi_o3_1h, udf_aqi_co, udf_aqi_so2,
    udf_aqi_no2, udf_aqi_pm10, udf_aqi_pm25,
    udf_aqi_category, udf_aqi_color, udf_health_advice,
)

# ── Schema message Kafka ────────────────────────────────────────
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

# ── UDF Nowcast ─────────────────────────────────────────────────
nowcast_udf = udf(
    lambda c: float(c) if c is not None else 0.0,
    DoubleType()
)

# ── Ghi vào HDFS ───────────────────────────────────────────────
def write_to_hdfs(batch_df, epoch_id):
    count = batch_df.count()
    if count == 0:
        print(f"Batch {epoch_id}: không có data")
        return
    batch_df \
        .withColumn("year",  year("event_time")) \
        .withColumn("month", month("event_time")) \
        .withColumn("day",   dayofmonth("event_time")) \
        .write \
        .mode("append") \
        .partitionBy("region", "year", "month", "day") \
        .parquet(HDFS_RAW_PATH)
    print(f"Batch {epoch_id}: ghi {count} records → HDFS")

# ── Ghi vào PostgreSQL ─────────────────────────────────────────
def write_to_postgres(batch_df, epoch_id):
    count = batch_df.count()
    if count == 0:
        return
    batch_df.write.jdbc(
        url        = POSTGRES_URL,
        table      = "aqi_readings",
        mode       = "append",
        properties = POSTGRES_PROPS,
    )
    print(f"Batch {epoch_id}: ghi {count} records → PostgreSQL")

# ── Main ────────────────────────────────────────────────────────
def main():
    spark = SparkSession.builder \
        .appName("VN-AQI-Streaming") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory",        SPARK_EXECUTOR_MEMORY) \
        .config("spark.hadoop.fs.defaultFS",    HDFS_NAMENODE) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session started")
    print(f"   Kafka      : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   HDFS       : {HDFS_RAW_PATH}")
    print(f"   PostgreSQL : {POSTGRES_URL}\n")

    # 1. Đọc từ Kafka
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",               KAFKA_TOPIC_RAW) \
        .option("startingOffsets",         "latest") \
        .option("failOnDataLoss",          "false") \
        .load()

    # 2. Parse JSON
    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*") \
     .withColumn("event_time", to_timestamp("timestamp")) \
     .filter(col("event_time").isNotNull()) \
     .filter(col("pm2_5").isNotNull() | col("pm10").isNotNull())

    # 3. Tính Nowcast PM2.5, PM10
    with_nowcast = parsed \
        .withColumn("nowcast_pm25", nowcast_udf("pm2_5")) \
        .withColumn("nowcast_pm10", nowcast_udf("pm10"))

    # 4. Tính AQI từng thông số theo VN_AQI
    enriched = with_nowcast \
        .withColumn("aqi_o3",       udf_aqi_o3_1h("o3")) \
        .withColumn("aqi_co",       udf_aqi_co("co")) \
        .withColumn("aqi_so2",      udf_aqi_so2("so2")) \
        .withColumn("aqi_no2",      udf_aqi_no2("no2")) \
        .withColumn("aqi_pm10",     udf_aqi_pm10("nowcast_pm10")) \
        .withColumn("aqi_pm25",     udf_aqi_pm25("nowcast_pm25")) \
        .withColumn("aqi_final",
            greatest("aqi_o3", "aqi_co", "aqi_so2",
                     "aqi_no2", "aqi_pm10", "aqi_pm25")) \
        .withColumn("aqi_category",  udf_aqi_category("aqi_final")) \
        .withColumn("aqi_color",     udf_aqi_color("aqi_final")) \
        .withColumn("health_advice", udf_health_advice("aqi_final")) \
        .withColumn("ingested_at",   current_timestamp()) \
        .drop("timestamp")

    # 5. Sink 1: HDFS
    query_hdfs = enriched.writeStream \
        .foreachBatch(write_to_hdfs) \
        .outputMode("append") \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT}/hdfs") \
        .trigger(processingTime=f"{TRIGGER_HDFS_SECONDS} seconds") \
        .start()

    # 6. Sink 2: PostgreSQL
    query_sql = enriched.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT}/postgres") \
        .trigger(processingTime=f"{TRIGGER_SQL_SECONDS} seconds") \
        .start()

    print("Spark Streaming đang chạy...")
    print(f"   → HDFS      : ghi mỗi {TRIGGER_HDFS_SECONDS}s")
    print(f"   → PostgreSQL: ghi mỗi {TRIGGER_SQL_SECONDS}s")
    print("   Nhấn Ctrl+C để dừng\n")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()