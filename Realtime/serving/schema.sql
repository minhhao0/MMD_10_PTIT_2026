-- ══════════════════════════════════════════════════════════════
--  SCHEMA.SQL — PostgreSQL schema cho hệ thống AQI Vietnam
--  Chạy: docker exec -i postgres psql -U aqi_user -d aqi_db < schema.sql
-- ══════════════════════════════════════════════════════════════

-- ── Bảng chính lưu toàn bộ readings ───────────────────────────
CREATE TABLE IF NOT EXISTS aqi_readings (
    id             BIGSERIAL PRIMARY KEY,
    province       VARCHAR(100) NOT NULL,
    district       VARCHAR(100) NOT NULL,
    region         VARCHAR(20),
    lat            DOUBLE PRECISION,
    lon            DOUBLE PRECISION,
    -- Giờ VN từ Open-Meteo (không lưu UTC — tránh hiển thị lệch 7h)
    event_time     TIMESTAMP NOT NULL,
    ingested_at    TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
    -- Chỉ số thô (µg/m³)
    pm2_5          DOUBLE PRECISION,
    pm10           DOUBLE PRECISION,
    o3             DOUBLE PRECISION,
    no2            DOUBLE PRECISION,
    so2            DOUBLE PRECISION,
    co             DOUBLE PRECISION,
    -- Nowcast PM2.5, PM10 (tính từ 12 giờ lịch sử)
    nowcast_pm25   DOUBLE PRECISION,
    nowcast_pm10   DOUBLE PRECISION,
    -- AQI từng thông số (VN_AQI QĐ-TCMT 2019)
    aqi_pm25       INTEGER,
    aqi_pm10       INTEGER,
    aqi_o3         INTEGER,
    aqi_no2        INTEGER,
    aqi_so2        INTEGER,
    aqi_co         INTEGER,
    -- AQI tổng hợp
    aqi_final      INTEGER,
    aqi_category   VARCHAR(50),
    aqi_color      VARCHAR(10),
    health_advice  VARCHAR(500)
);

-- ── Index tối ưu dashboard query ──────────────────────────────
CREATE INDEX IF NOT EXISTS idx_province_time
    ON aqi_readings (province, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_district
    ON aqi_readings (district);

CREATE INDEX IF NOT EXISTS idx_event_time
    ON aqi_readings (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_aqi_final
    ON aqi_readings (aqi_final DESC);

CREATE INDEX IF NOT EXISTS idx_region
    ON aqi_readings (region, event_time DESC);

-- ── View: bản ghi mới nhất mỗi quận/huyện ─────────────────────
-- Dashboard dùng view này hiển thị real-time
CREATE OR REPLACE VIEW aqi_latest AS
SELECT DISTINCT ON (province, district)
    province, district, region, lat, lon,
    event_time, aqi_final, aqi_category,
    aqi_color, health_advice,
    pm2_5, pm10, o3, no2, so2, co,
    nowcast_pm25, nowcast_pm10,
    aqi_pm25, aqi_pm10, aqi_o3, aqi_no2, aqi_so2, aqi_co
FROM aqi_readings
ORDER BY province, district, event_time DESC;

-- ── View: thống kê AQI theo tỉnh ──────────────────────────────
CREATE OR REPLACE VIEW aqi_by_province AS
SELECT
    province,
    region,
    COUNT(DISTINCT district)          AS district_count,
    ROUND(AVG(aqi_final)::NUMERIC, 0) AS avg_aqi,
    MAX(aqi_final)                    AS max_aqi,
    MIN(aqi_final)                    AS min_aqi,
    MAX(event_time)                   AS last_updated
FROM aqi_latest
GROUP BY province, region
ORDER BY avg_aqi DESC;

-- ── View: top 10 khu vực ô nhiễm nhất ─────────────────────────
CREATE OR REPLACE VIEW aqi_top_polluted AS
SELECT
    province, district, region,
    aqi_final, aqi_category, aqi_color,
    health_advice, event_time
FROM aqi_latest
ORDER BY aqi_final DESC
LIMIT 10;

-- ── Bảng hourly: trung bình theo giờ 7 ngày gần nhất ──────────
-- Spark batch job ghi vào đây mỗi giờ
CREATE TABLE IF NOT EXISTS aqi_hourly (
    province     VARCHAR(100),
    district     VARCHAR(100),
    region       VARCHAR(20),
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    hour_bucket  TIMESTAMP,
    avg_aqi      INTEGER,
    max_aqi      INTEGER,
    avg_pm25     DOUBLE PRECISION,
    avg_pm10     DOUBLE PRECISION,
    aqi_category VARCHAR(50),
    PRIMARY KEY (province, district, hour_bucket)
);

CREATE INDEX IF NOT EXISTS idx_hourly_time
    ON aqi_hourly (hour_bucket DESC);

CREATE INDEX IF NOT EXISTS idx_hourly_prov
    ON aqi_hourly (province, hour_bucket DESC);

\echo 'Schema tạo thành công!'
\echo 'Tables : aqi_readings, aqi_hourly'
\echo 'Views  : aqi_latest, aqi_by_province, aqi_top_polluted'