# Tạo lại topic aqi-raw (3 partitions) — chạy khi checkpoint/topic lệch
# Usage: .\scripts\reset_kafka_topic.ps1

$ErrorActionPreference = "Stop"
$Topic = "aqi-raw"

Write-Host "=== Xoa topic $Topic (neu co) ===" -ForegroundColor Cyan
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $Topic 2>$null
Start-Sleep -Seconds 3

Write-Host "=== Tao topic $Topic (3 partitions) ===" -ForegroundColor Cyan
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --create --topic $Topic --partitions 3 --replication-factor 1

Write-Host "`n=== Thong tin topic ===" -ForegroundColor Green
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $Topic
