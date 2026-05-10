
-- Tạo database nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'aqi_db')
BEGIN
    CREATE DATABASE aqi_db;
END
GO

USE aqi_db;
GO

-- Bảng chính lưu toàn bộ readings 
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'aqi_readings')
BEGIN
    CREATE TABLE dbo.aqi_readings (
        id             BIGINT IDENTITY(1,1) PRIMARY KEY,
        -- Thông tin vị trí
        province       NVARCHAR(100) NOT NULL,
        district       NVARCHAR(100) NOT NULL,
        region         NVARCHAR(20),
        lat            FLOAT,
        lon            FLOAT,
        -- Thời gian
        event_time     DATETIME2 NOT NULL,
        ingested_at    DATETIME2 DEFAULT GETDATE(),
        -- Chỉ số thô (µg/m³)
        pm2_5          FLOAT,
        pm10           FLOAT,
        o3             FLOAT,
        no2            FLOAT,
        so2            FLOAT,
        co             FLOAT,
        -- Nowcast (tính từ 12 giờ lịch sử)
        nowcast_pm25   FLOAT,
        nowcast_pm10   FLOAT,
        -- AQI từng thông số (theo VN_AQI QĐ-TCMT 2019)
        aqi_pm25       INT,
        aqi_pm10       INT,
        aqi_o3         INT,
        aqi_no2        INT,
        aqi_so2        INT,
        aqi_co         INT,
        -- AQI tổng hợp
        aqi_final      INT,
        aqi_category   NVARCHAR(50),   -- Tốt/Trung bình/Kém/Xấu/Rất xấu/Nguy hại
        aqi_color      NVARCHAR(10),   -- Mã màu hex
        health_advice  NVARCHAR(500),  -- Khuyến nghị sức khỏe
    );
END
GO

-- Index tối ưu query cho dashboard 
CREATE INDEX idx_province_time
    ON dbo.aqi_readings (province, event_time DESC);

CREATE INDEX idx_district
    ON dbo.aqi_readings (district);

CREATE INDEX idx_event_time
    ON dbo.aqi_readings (event_time DESC);

CREATE INDEX idx_aqi_final
    ON dbo.aqi_readings (aqi_final DESC);

CREATE INDEX idx_region
    ON dbo.aqi_readings (region, event_time DESC);
GO

-- View: lấy bản ghi mới nhất mỗi quận/huyện 
-- Dashboard dùng view này để hiển thị real-time
CREATE OR ALTER VIEW dbo.aqi_latest AS
SELECT
    province, district, region, lat, lon,
    event_time, aqi_final, aqi_category,
    aqi_color, health_advice,
    pm2_5, pm10, o3, no2, so2, co,
    aqi_pm25, aqi_pm10, aqi_o3, aqi_no2, aqi_so2, aqi_co
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY province, district
            ORDER BY event_time DESC
        ) AS rn
    FROM dbo.aqi_readings
) ranked
WHERE rn = 1;
GO

-- View: thống kê theo tỉnh 
CREATE OR ALTER VIEW dbo.aqi_by_province AS
SELECT
    province,
    region,
    COUNT(DISTINCT district)    AS district_count,
    AVG(CAST(aqi_final AS FLOAT)) AS avg_aqi,
    MAX(aqi_final)              AS max_aqi,
    MIN(aqi_final)              AS min_aqi,
    MAX(event_time)             AS last_updated
FROM dbo.aqi_latest
GROUP BY province, region;
GO

-- View: top 10 khu vực ô nhiễm nhất 
CREATE OR ALTER VIEW dbo.aqi_top_polluted AS
SELECT TOP 10
    province, district, region,
    aqi_final, aqi_category, aqi_color,
    health_advice, event_time
FROM dbo.aqi_latest
ORDER BY aqi_final DESC;
GO

-- Stored Procedure: lấy lịch sử AQI theo quận/huyện 
CREATE OR ALTER PROCEDURE dbo.sp_get_aqi_history
    @province  NVARCHAR(100),
    @district  NVARCHAR(100),
    @hours     INT = 24
AS
BEGIN
    SELECT
        event_time, aqi_final, aqi_category,
        pm2_5, pm10, o3, no2, so2, co
    FROM dbo.aqi_readings
    WHERE province = @province
      AND district = @district
      AND event_time >= DATEADD(HOUR, -@hours, GETDATE())
    ORDER BY event_time ASC;
END
GO

PRINT 'Schema tạo thành công!';
PRINT 'Tables: aqi_readings';
PRINT 'Views : aqi_latest, aqi_by_province, aqi_top_polluted';
PRINT 'Procs : sp_get_aqi_history';