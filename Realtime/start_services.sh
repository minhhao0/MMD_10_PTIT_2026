#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  START_SERVICES.SH — Khởi động toàn bộ services
#  Chạy trên máy ảo Ubuntu sau khi đã cài xong setup.sh
# ══════════════════════════════════════════════════════════════

KAFKA_HOME=/opt/kafka
HADOOP_HOME=/opt/hadoop
SPARK_HOME=/opt/spark

echo "======================================================"
echo " Khởi động Zookeeper"
echo "======================================================"
$KAFKA_HOME/bin/zookeeper-server-start.sh \
    $KAFKA_HOME/config/zookeeper.properties &
sleep 5
echo "Zookeeper đang chạy"

echo ""
echo "======================================================"
echo " Khởi động Kafka Broker"
echo "======================================================"
$KAFKA_HOME/bin/kafka-server-start.sh \
    $KAFKA_HOME/config/server.properties &
sleep 5
echo "Kafka đang chạy"

# Tạo topic nếu chưa có
$KAFKA_HOME/bin/kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic aqi-raw \
    --partitions 3 \
    --replication-factor 1
echo "Topic aqi-raw đã tạo"

echo ""
echo "======================================================"
echo " Khởi động Hadoop HDFS"
echo "======================================================"
$HADOOP_HOME/sbin/start-dfs.sh
sleep 5

# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /aqi/raw
hdfs dfs -mkdir -p /aqi/checkpoints
echo "HDFS đang chạy, thư mục /aqi đã tạo"

echo ""
echo "======================================================"
echo " Khởi động Spark"
echo "======================================================"
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh
echo "Spark đang chạy"

echo ""
echo "======================================================"
echo " Tất cả services đã khởi động!"
echo "======================================================"
echo " Kafka    : localhost:9092"
echo " HDFS     : hdfs://localhost:9000"
echo " Spark UI : http://localhost:8080"
echo ""
echo " Bước tiếp theo:"
echo " 1. Chạy fetcher : python collect/fetcher.py"
echo " 2. Chạy Spark   : python processing/spark_aqi.py"