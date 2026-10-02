"""
Streamlit dashboard for the weather ELT pipeline.

Reads directly from the same Postgres warehouse the Airflow pipeline writes
to (weather_hourly, weather_daily_summary) — no separate data path, so the
dashboard always reflects whatever the last pipeline run produced.

Run locally with:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py

Assumes Postgres is reachable at localhost:5432 (the default from
docker-compose.yml, which publishes that port to your host machine).
"""
import pandas as pd
import psycopg2
import streamlit as st

DB_PARAMS = dict(
    host="localhost", port=5432, dbname="airflow", user="airflow", password="airflow"
)

st.set_page_config(page_title="Weather ELT Dashboard", layout="wide")


@st.cache_resource
def get_connection():
    # A plain psycopg2 connection avoids pandas/SQLAlchemy version-matching
    # issues entirely — pandas.read_sql works directly against a DBAPI
    # connection like this one.
    return psycopg2.connect(**DB_PARAMS)


@st.cache_data(ttl=60)
def load_daily(_conn) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM weather_daily_summary ORDER BY observed_date", _conn
    )


@st.cache_data(ttl=60)
def load_current(_conn) -> pd.Series:
    # weather_hourly holds both recent-past AND forecasted future hours
    # (extract.py asks Open-Meteo for forecast_days=1), so "the newest
    # timestamp in the table" is usually a future forecast, not now. Pick
    # the most recent row that isn't in the future instead.
    df = pd.read_sql(
        """
        SELECT * FROM weather_hourly
        WHERE observed_at <= (now() AT TIME ZONE 'America/Winnipeg')
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        _conn,
    )
    return df.iloc[0] if not df.empty else None


@st.cache_data(ttl=60)
def load_hourly(_conn, hours: int = 48) -> pd.DataFrame:
    return pd.read_sql(
        f"""
        SELECT * FROM weather_hourly
        ORDER BY observed_at DESC
        LIMIT {hours}
        """,
        _conn,
    ).sort_values("observed_at")


engine = get_connection()

st.title("Winnipeg weather — ELT pipeline output")
st.caption(
    "Data fetched hourly from Open-Meteo by an Airflow DAG, "
    "landed raw in Postgres, and transformed into the tables below."
)

try:
    daily = load_daily(engine)
    hourly = load_hourly(engine)
except Exception as e:
    st.error(
        "Couldn't connect to Postgres. Make sure `docker compose up` is "
        "running and the pipeline has completed at least one run."
    )
    st.exception(e)
    st.stop()

if daily.empty or hourly.empty:
    st.warning(
        "No data yet — trigger the `weather_elt_pipeline` DAG in Airflow "
        "(http://localhost:8080) and refresh this page."
    )
    st.stop()

current = load_current(engine)
if current is None:
    st.warning("No reading at or before the current time yet.")
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current conditions as of", current["observed_at"].strftime("%b %d, %H:%M"))
    col2.metric("Temperature", f"{current['temperature_c']:.1f} °C")
    col3.metric("Wind speed", f"{current['wind_speed_kmh']:.1f} km/h")
    col4.metric("Precipitation", f"{current['precipitation_mm']:.1f} mm")
    st.caption(
        "The hourly table also includes tomorrow's forecast "
        "(see the charts and raw table below)."
    )

st.subheader("Daily summary")
daily_indexed = daily.set_index("observed_date")
st.line_chart(
    daily_indexed[["avg_temperature_c", "max_temperature_c", "min_temperature_c"]]
)
st.bar_chart(daily_indexed[["total_precipitation_mm"]])

st.subheader(f"Hourly detail (last {len(hourly)} readings)")
hourly_indexed = hourly.set_index("observed_at")
st.subheader(f"Temparature(C) ")
st.line_chart(hourly_indexed[["temperature_c"]])
st.subheader(f"Wind Speed (kmph) ")
st.line_chart(hourly_indexed[["wind_speed_kmh"]])

with st.expander("Raw table"):
    st.dataframe(hourly.sort_values("observed_at", ascending=False), use_container_width=True)

st.caption("Refreshes every 60 seconds · reload the page to force an update.")