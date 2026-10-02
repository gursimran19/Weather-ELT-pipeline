-- Raw landing zone: one row per pipeline run, full API payload kept as JSONB
CREATE TABLE IF NOT EXISTS raw_weather (
    id              SERIAL PRIMARY KEY,
    fetched_at      TIMESTAMP NOT NULL DEFAULT now(),
    city            TEXT NOT NULL,
    payload         JSONB NOT NULL
);

-- Cleaned / transformed warehouse table: one row per city per hour
CREATE TABLE IF NOT EXISTS weather_hourly (
    city            TEXT NOT NULL,
    observed_at     TIMESTAMP NOT NULL,
    temperature_c   NUMERIC,
    precipitation_mm NUMERIC,
    wind_speed_kmh  NUMERIC,
    weather_code    INTEGER,
    loaded_at       TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (city, observed_at)
);

-- Daily rollup, built on top of weather_hourly — good base for a dashboard
CREATE TABLE IF NOT EXISTS weather_daily_summary (
    city                TEXT NOT NULL,
    observed_date       DATE NOT NULL,
    avg_temperature_c   NUMERIC,
    max_temperature_c   NUMERIC,
    min_temperature_c   NUMERIC,
    total_precipitation_mm NUMERIC,
    max_wind_speed_kmh  NUMERIC,
    PRIMARY KEY (city, observed_date)
);
