"""
Transform step:
1. Unpack unprocessed rows in raw_weather (JSONB) into the clean, typed
   weather_hourly table (upsert, so re-runs are safe).
2. Roll weather_hourly up into weather_daily_summary.

This keeps "raw" and "clean" separated, which is the core ELT pattern:
load everything as-is first, transform afterwards, and re-run transforms
freely without re-hitting the API.
"""
import os

from sqlalchemy import create_engine, text

DB_URI = os.environ["WAREHOUSE_DB_URI"]

UPSERT_HOURLY_SQL = text(
    """
    WITH expanded AS (
        SELECT
            r.fetched_at,
            city,
            (hourly ->> 'time')::timestamp AS observed_at,
            (hourly ->> 'temperature')::numeric AS temperature_c,
            (hourly ->> 'precipitation')::numeric AS precipitation_mm,
            (hourly ->> 'wind_speed')::numeric AS wind_speed_kmh,
            (hourly ->> 'weather_code')::integer AS weather_code
        FROM raw_weather r,
        LATERAL (
            SELECT
                jsonb_build_object(
                    'time', t.time,
                    'temperature', t.temperature,
                    'precipitation', t.precipitation,
                    'wind_speed', t.wind_speed,
                    'weather_code', t.weather_code
                ) AS hourly
            FROM (
                SELECT
                    jsonb_array_elements_text(r.payload -> 'hourly' -> 'time') AS time,
                    jsonb_array_elements_text(r.payload -> 'hourly' -> 'temperature_2m') AS temperature,
                    jsonb_array_elements_text(r.payload -> 'hourly' -> 'precipitation') AS precipitation,
                    jsonb_array_elements_text(r.payload -> 'hourly' -> 'wind_speed_10m') AS wind_speed,
                    jsonb_array_elements_text(r.payload -> 'hourly' -> 'weather_code') AS weather_code
            ) t
        ) hourly_row
    ),
    -- Different pipeline runs can fetch overlapping hours (extract.py always
    -- asks for "past_days=1, forecast_days=1"). Keep only the most recently
    -- fetched value for each (city, observed_at) so the INSERT below never
    -- targets the same conflict row twice in one statement.
    deduped AS (
        SELECT DISTINCT ON (city, observed_at)
            city, observed_at, temperature_c, precipitation_mm, wind_speed_kmh, weather_code
        FROM expanded
        ORDER BY city, observed_at, fetched_at DESC
    )
    INSERT INTO weather_hourly (city, observed_at, temperature_c, precipitation_mm, wind_speed_kmh, weather_code)
    SELECT city, observed_at, temperature_c, precipitation_mm, wind_speed_kmh, weather_code
    FROM deduped
    ON CONFLICT (city, observed_at) DO UPDATE SET
        temperature_c = EXCLUDED.temperature_c,
        precipitation_mm = EXCLUDED.precipitation_mm,
        wind_speed_kmh = EXCLUDED.wind_speed_kmh,
        weather_code = EXCLUDED.weather_code,
        loaded_at = now();
    """
)

UPSERT_DAILY_SQL = text(
    """
    INSERT INTO weather_daily_summary (
        city, observed_date, avg_temperature_c, max_temperature_c,
        min_temperature_c, total_precipitation_mm, max_wind_speed_kmh
    )
    SELECT
        city,
        observed_at::date AS observed_date,
        AVG(temperature_c),
        MAX(temperature_c),
        MIN(temperature_c),
        SUM(precipitation_mm),
        MAX(wind_speed_kmh)
    FROM weather_hourly
    GROUP BY city, observed_at::date
    ON CONFLICT (city, observed_date) DO UPDATE SET
        avg_temperature_c = EXCLUDED.avg_temperature_c,
        max_temperature_c = EXCLUDED.max_temperature_c,
        min_temperature_c = EXCLUDED.min_temperature_c,
        total_precipitation_mm = EXCLUDED.total_precipitation_mm,
        max_wind_speed_kmh = EXCLUDED.max_wind_speed_kmh;
    """
)


def run_transform() -> None:
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(UPSERT_HOURLY_SQL)
        conn.execute(UPSERT_DAILY_SQL)


if __name__ == "__main__":
    run_transform()
    print("Transform complete: weather_hourly and weather_daily_summary updated")