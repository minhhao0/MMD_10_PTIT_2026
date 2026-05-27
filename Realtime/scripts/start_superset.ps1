# Khởi tạo DB metadata + chạy Apache Superset
# Usage: .\scripts\start_superset.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

Write-Host "=== Tao database superset_meta (neu chua co) ===" -ForegroundColor Cyan
$exists = docker exec postgres psql -U aqi_user -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='superset_meta'"
if ($exists -ne "1") {
    docker exec postgres psql -U aqi_user -d postgres -c "CREATE DATABASE superset_meta;"
    Write-Host "Da tao superset_meta"
} else {
    Write-Host "superset_meta da ton tai"
}

Write-Host "`n=== Khoi dong Redis + Superset (lan dau co the mat 2-3 phut) ===" -ForegroundColor Cyan
docker compose up -d redis
docker compose up superset-init
docker compose up -d superset

Write-Host "`n=== Superset san sang ===" -ForegroundColor Green
Write-Host "  URL      : http://localhost:8088"
Write-Host "  User     : admin"
Write-Host "  Password : admin"
Write-Host ""
Write-Host "Buoc tiep theo: mo serving/SUPERSET.md de ket noi aqi_db va tao dashboard."
