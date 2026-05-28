# Khởi tạo DB metadata + chạy Apache Superset
# Usage: .\scripts\start_superset.ps1
# Cần file .env (POSTGRES_USER, ...) khớp docker-compose.yml

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

function Get-EnvValue([string]$Name) {
    $line = Get-Content (Join-Path $Root ".env") -ErrorAction Stop |
        Where-Object { $_ -match "^\s*$([regex]::Escape($Name))\s*=" } |
        Select-Object -First 1
    if (-not $line) { throw "Thieu $Name trong .env" }
    return ($line -split "=", 2)[1].Trim()
}

$pgUser = Get-EnvValue "POSTGRES_USER"

Write-Host "=== Tao database superset_meta (neu chua co) ===" -ForegroundColor Cyan
$exists = docker exec postgres psql -U $pgUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='superset_meta'"
if ($exists -ne "1") {
    docker exec postgres psql -U $pgUser -d postgres -c "CREATE DATABASE superset_meta;"
    Write-Host "Da tao superset_meta"
} else {
    Write-Host "superset_meta da ton tai"
}

Write-Host "`n=== Khoi dong Redis + Superset (lan dau co the mat 2-3 phut) ===" -ForegroundColor Cyan
docker compose up -d redis
docker compose up superset-init
docker compose up -d superset

Write-Host "`n=== Superset san sang ===" -ForegroundColor Green
Write-Host "  URL: http://localhost:8088"
Write-Host "  Tai khoan admin: xem lenh superset-init trong docker-compose.yml"
Write-Host "  Ket noi aqi_db: dung POSTGRES_* trong file .env cua ban"
