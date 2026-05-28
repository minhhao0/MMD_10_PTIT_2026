# Xoa checkpoint Spark Streaming (chay truoc khi start spark_aqi.py lan dau / sau khi doi topic)
# Usage: .\scripts\reset_checkpoints.ps1

$ErrorActionPreference = "Stop"
$Base = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Paths = @(
    "$Base\data\checkpoints\hdfs",
    "$Base\data\checkpoints\postgres",
    "$Base\data\checkpoints\streaming"
)

foreach ($p in $Paths) {
    if (Test-Path $p) {
        Remove-Item -Recurse -Force $p
        Write-Host "Da xoa: $p" -ForegroundColor Yellow
    } else {
        Write-Host "Khong co: $p" -ForegroundColor DarkGray
    }
}
Write-Host "`nCheckpoint da don. Co the chay spark_aqi.py." -ForegroundColor Green
