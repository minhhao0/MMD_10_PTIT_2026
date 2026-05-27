import sys
import os
from pathlib import Path

# Đảm bảo Python có thể import được file config.py ở thư mục cha (nếu cần)
sys.path.append(str(Path(__file__).parent.parent))

# ── Cấu hình môi trường Windows ────────────────────────────────
os.environ["JAVA_HOME"]       = r"C:\Users\TGDD\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"]     = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["SPARK_HOME"]      = r"D:\spark-4.1.1-bin-hadoop3"
os.environ["PATH"]            = (
    os.environ["JAVA_HOME"]   + r"\bin;" +
    os.environ["HADOOP_HOME"] + r"\bin;" +
    os.environ["SPARK_HOME"]  + r"\bin;" +
    os.environ.get("PATH", "")
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW,
    HDFS_RAW_PATH, HDFS_NAMENODE,
    POSTGRES_URL, POSTGRES_PROPS,
    SPARK_MASTER, SPARK_DRIVER_MEMORY,
    SPARK_SHUFFLE_PARTITIONS,
    TRIGGER_HDFS_SECONDS,
    TIMEZONE,
)

# Open-Meteo trả về "2026-05-26T14:00" = giờ địa phương VN (không có offset)
EVENT_TIME_FMT = "yyyy-MM-dd'T'HH:mm"

# ── Checkpoint (một stream = một consumer Kafka) ───────────────
CHECKPOINT_DIR = "file:///C:/Users/TGDD/.vscode/MMD/MMD_10_PTIT_2026/Realtime/data/checkpoints/streaming"
KAFKA_MAX_OFFSETS_PER_TRIGGER = "500"

# ── Retention SQL: chỉ giữ N ngày gần nhất ─────────────────────
POSTGRES_RETENTION_DAYS = 7

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

# ══════════════════════════════════════════════════════════════
#  Tính AQI bằng Spark SQL expressions (KHÔNG dùng Python UDF)
#  Theo QĐ-TCMT 2019 — piecewise linear interpolation
#  AQIx = ((I_hi - I_lo) / (BP_hi - BP_lo)) * (C - BP_lo) + I_lo
# ══════════════════════════════════════════════════════════════

def aqi_from_col(c, breakpoints):
    """
    Tạo Spark SQL expression tính AQI từ 1 cột.
    breakpoints: list of (bp_lo, bp_hi, i_lo, i_hi)
    Dùng nested when/otherwise — không cần Python UDF.
    """
    expr = lit(0)
    for (bp_lo, bp_hi, i_lo, i_hi) in reversed(breakpoints):
        if bp_hi != bp_lo:  # Tránh lỗi chia cho 0
            slope = (i_hi - i_lo) / (bp_hi - bp_lo)
            val   = (slope * (c - bp_lo) + i_lo).cast("int")
            expr  = when((c >= bp_lo) & (c <= bp_hi), val).otherwise(expr)
            
    # Vượt ngưỡng tối đa
    expr = when(c > breakpoints[-1][1], lit(500)).otherwise(expr)
    return expr

# Bảng breakpoint VN_AQI (QĐ-TCMT 2019)
BP_O3   = [(0,160,0,50),(160,200,50,100),(200,300,100,150),(300,400,150,200),(400,800,200,300),(800,1000,300,400),(1000,1200,400,500)]
BP_CO   = [(0,10000,0,50),(10000,30000,50,100),(30000,45000,100,150),(45000,60000,150,200),(60000,90000,200,300),(90000,120000,300,400),(120000,150000,400,500)]
BP_SO2  = [(0,125,0,50),(125,350,50,100),(350,550,100,150),(550,800,150,200),(800,1600,200,300),(1600,2100,300,400),(2100,2630,400,500)]
BP_NO2  = [(0,100,0,50),(100,200,50,100),(200,700,100,150),(700,1200,150,200),(1200,2350,200,300),(2350,3100,300,400),(3100,3850,400,500)]
BP_PM10 = [(0,50,0,50),(50,150,50,100),(150,250,100,150),(250,350,150,200),(350,420,200,300),(420,500,300,400),(500,600,400,500)]
BP_PM25 = [(0,25,0,50),(25,50,50,100),(50,80,100,150),(80,150,150,200),(150,250,200,300),(250,350,300,400),(350,500,400,500)]

def aqi_category_expr(c):
    return (
        when(c <= 50,  lit("Tot"))
        .when(c <= 100, lit("Trung binh"))
        .when(c <= 150, lit("Kem"))
        .when(c <= 200, lit("Xau"))
        .when(c <= 300, lit("Rat xau"))
        .otherwise(lit("Nguy hai"))
    )

def aqi_color_expr(c):
    return (
        when(c <= 50,  lit("#00E400"))
        .when(c <= 100, lit("#FFFF00"))
        .when(c <= 150, lit("#FF7E00"))
        .when(c <= 200, lit("#FF0000"))
        .when(c <= 300, lit("#8F3F97"))
        .otherwise(lit("#7E0023"))
    )

def health_advice_expr(c):
    return (
        when(c <= 50,  lit("Chat luong KK tot. Tu do hoat dong ngoai troi."))
        .when(c <= 100, lit("Chap nhan duoc. Nguoi nhay cam nen theo doi."))
        .when(c <= 150, lit("Nguoi nhay cam nen giam hoat dong ngoai troi."))
        .when(c <= 200, lit("Moi nguoi nen giam hoat dong ngoai troi, deo khau trang."))
        .when(c <= 300, lit("Han che toi da ngoai troi. Deo khau trang khi ra ngoai."))
        .otherwise(lit("O trong nha, dong cua so. Bat buoc deo khau trang neu ra ngoai."))
    )

def _has_rows(batch_df) -> bool:
    """Kiểm tra batch có dữ liệu (nhẹ hơn isEmpty trong foreachBatch)."""
    return len(batch_df.take(1)) > 0

def write_to_hdfs(batch_df, epoch_id):
    batch_df \
        .withColumn("year",  year("event_time")) \
        .withColumn("month", month("event_time")) \
        .withColumn("day",   dayofmonth("event_time")) \
        .write \
        .mode("append") \
        .partitionBy("region", "year", "month", "day") \
        .parquet(HDFS_RAW_PATH)
    print(f"  Batch {epoch_id}: ghi vao HDFS")

def write_to_postgres(batch_df, epoch_id):
    batch_df.write.jdbc(
        url        = POSTGRES_URL,
        table      = "aqi_readings",
        mode       = "append",
        properties = POSTGRES_PROPS,
    )
    print(f"  Batch {epoch_id}: ghi vao PostgreSQL")

def prune_postgres_retention(spark: SparkSession, days: int = POSTGRES_RETENTION_DAYS):
    """
    Xóa dữ liệu cũ trong PostgreSQL để DB chỉ giữ N ngày gần nhất.
    Chạy bằng JDBC (qua JVM) để không cần cài thêm thư viện Python.
    """
    # Dùng event_time làm mốc thời gian dữ liệu
    sql = f"DELETE FROM aqi_readings WHERE event_time < (NOW() - INTERVAL '{int(days)} days')"

    # Lấy JVM từ SparkSession hiện tại (an toàn trong foreachBatch)
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_PROPS.get("user", "aqi_user"))
    props.setProperty("password", POSTGRES_PROPS.get("password", "aqi_pass"))

    conn = None
    stmt = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(POSTGRES_URL, props)
        stmt = conn.createStatement()
        deleted = stmt.executeUpdate(sql)
        if deleted:
            print(f"  Retention: da xoa {deleted} dong cu (> {days} ngay)")
    except Exception as e:
        # Không để retention làm hỏng streaming
        print(f"  Retention WARN: {e}")
    finally:
        if stmt is not None:
            try:
                stmt.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def write_both_sinks(batch_df, epoch_id):
    """Một consumer Kafka — ghi song song HDFS + PostgreSQL."""
    if not _has_rows(batch_df):
        print(f"Batch {epoch_id}: khong co data")
        return
    write_to_hdfs(batch_df, epoch_id)
    write_to_postgres(batch_df, epoch_id)
    prune_postgres_retention(batch_df.sparkSession)

# ── Main ────────────────────────────────────────────────────────
def main():
    spark = SparkSession.builder \
        .appName("VN-AQI-Streaming") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY) \
        .config("spark.hadoop.fs.defaultFS",    HDFS_NAMENODE) \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.sql.streaming.metricsEnabled", "false") \
        .config("spark.sql.streaming.ui.enabled", "false") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.sql.session.timeZone", TIMEZONE) \
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={TIMEZONE}") \
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={TIMEZONE}") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark Session started")
    print(f"   Mode       : {SPARK_MASTER}")
    print(f"   Kafka      : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   HDFS       : {HDFS_RAW_PATH}")
    print(f"   PostgreSQL : {POSTGRES_URL}\n")
    
    # ── 1. Đọc từ Kafka ─────────────────────────────────────────
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",               KAFKA_TOPIC_RAW) \
        .option("startingOffsets",         "latest") \
        .option("failOnDataLoss",          "false") \
        .option("includeHeaders",          "false") \
        .option("maxOffsetsPerTrigger",    KAFKA_MAX_OFFSETS_PER_TRIGGER) \
        .option("kafka.request.timeout.ms",      "120000") \
        .option("kafka.session.timeout.ms",      "60000") \
        .option("kafka.metadata.max.age.ms",     "300000") \
        .load()
        
    # ── 2. Parse JSON ────────────────────────────────────────────
    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*") \
     .withColumn(
         "event_time",
         to_timestamp(col("timestamp"), EVENT_TIME_FMT),
     ) \
     .filter(col("event_time").isNotNull()) \
     .filter(col("pm2_5").isNotNull() | col("pm10").isNotNull())
     
    # ── 3. Nowcast = giá trị hiện tại (streaming) ───────────────
    with_nowcast = parsed \
        .withColumn("nowcast_pm25", col("pm2_5")) \
        .withColumn("nowcast_pm10", col("pm10"))
        
    # ── 4. Tính AQI bằng Spark SQL (không dùng Python UDF) ──────
    enriched = with_nowcast \
        .withColumn("aqi_o3",   aqi_from_col(col("o3"),           BP_O3)) \
        .withColumn("aqi_co",   aqi_from_col(col("co"),           BP_CO)) \
        .withColumn("aqi_so2",  aqi_from_col(col("so2"),          BP_SO2)) \
        .withColumn("aqi_no2",  aqi_from_col(col("no2"),          BP_NO2)) \
        .withColumn("aqi_pm10", aqi_from_col(col("nowcast_pm10"), BP_PM10)) \
        .withColumn("aqi_pm25", aqi_from_col(col("nowcast_pm25"), BP_PM25)) \
        .withColumn("aqi_final", greatest("aqi_o3", "aqi_co", "aqi_so2", "aqi_no2", "aqi_pm10", "aqi_pm25")) \
        .withColumn("aqi_category",  aqi_category_expr(col("aqi_final"))) \
        .withColumn("aqi_color",     aqi_color_expr(col("aqi_final"))) \
        .withColumn("health_advice", health_advice_expr(col("aqi_final"))) \
        .withColumn("ingested_at",   current_timestamp()) \
        .drop("timestamp")
        
    # ── 5. Một stream → HDFS + PostgreSQL (tránh 2 consumer Kafka) ─
    query = enriched.writeStream \
        .foreachBatch(write_both_sinks) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime=f"{TRIGGER_HDFS_SECONDS} seconds") \
        .start()
        
    print("Spark Streaming dang chay...")
    print(f"   -> HDFS + PostgreSQL: ghi moi {TRIGGER_HDFS_SECONDS}s")
    print(f"   -> Checkpoint       : {CHECKPOINT_DIR}")
    print("   Nhan Ctrl+C de dung\n")
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()