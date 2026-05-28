-- Chuyển cột thời gian sang TIMESTAMP (giờ VN) nếu DB cũ dùng TIMESTAMPTZ.
-- Chạy: Get-Content scripts/migrate_timezone.sql | docker exec -i postgres psql -U aqi_user -d aqi_db

DROP VIEW IF EXISTS aqi_top_polluted CASCADE;
DROP VIEW IF EXISTS aqi_by_province CASCADE;
DROP VIEW IF EXISTS aqi_latest CASCADE;

ALTER TABLE aqi_readings
    ALTER COLUMN event_time TYPE TIMESTAMP
    USING event_time AT TIME ZONE 'Asia/Ho_Chi_Minh';

ALTER TABLE aqi_readings
    ALTER COLUMN ingested_at TYPE TIMESTAMP
    USING ingested_at AT TIME ZONE 'Asia/Ho_Chi_Minh';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'aqi_hourly'
    ) THEN
        ALTER TABLE aqi_hourly
            ALTER COLUMN hour_bucket TYPE TIMESTAMP
            USING hour_bucket AT TIME ZONE 'Asia/Ho_Chi_Minh';
    END IF;
END $$;

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

CREATE OR REPLACE VIEW aqi_top_polluted AS
SELECT
    province, district, region,
    aqi_final, aqi_category, aqi_color,
    health_advice, event_time
FROM aqi_latest
ORDER BY aqi_final DESC
LIMIT 10;

\echo 'Da chuyen event_time / hour_bucket sang gio VN (TIMESTAMP).'
