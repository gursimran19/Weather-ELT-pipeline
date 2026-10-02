"""
Load step: read raw JSON files written by extract.py and insert them into the
raw_weather staging table (JSONB column keeps the full payload intact).
"""
import glob
import json
import os

from sqlalchemy import create_engine, text

RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw")
DB_URI = os.environ["WAREHOUSE_DB_URI"]

INSERT_SQL = text(
    """
    INSERT INTO raw_weather (fetched_at, city, payload)
    VALUES (:fetched_at, :city, :payload)
    """
)


def load_all() -> int:
    engine = create_engine(DB_URI)
    files = sorted(glob.glob(os.path.join(RAW_DATA_DIR, "*.json")))

    loaded = 0
    with engine.begin() as conn:
        for path in files:
            with open(path) as f:
                record = json.load(f)

            conn.execute(
                INSERT_SQL,
                {
                    "fetched_at": record["fetched_at"],
                    "city": record["city"],
                    "payload": json.dumps(record["payload"]),
                },
            )
            loaded += 1
            # move processed file aside so it isn't loaded again next run
            os.rename(path, path + ".loaded")

    return loaded


if __name__ == "__main__":
    n = load_all()
    print(f"Loaded {n} raw record(s) into raw_weather")
