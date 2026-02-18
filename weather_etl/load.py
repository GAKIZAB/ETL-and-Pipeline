"""
Load module – persists transformed data to CSV files and a SQLite database.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("weather_etl.load")

# SQL DDL for the persistent table
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_current (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT    NOT NULL,
    timestamp       TEXT,
    temperature_c   REAL,
    windspeed_kmh   REAL,
    winddirection_deg REAL,
    weathercode     INTEGER,
    is_day          INTEGER,
    retrieval_timestamp TEXT NOT NULL
);
"""


def load_to_csv(
    df: pd.DataFrame,
    data_dir: str | Path = "data",
) -> Optional[Path]:
    """Write the DataFrame to a timestamped CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        Transformed weather data.
    data_dir : str | Path
        Directory where CSV files are stored.

    Returns
    -------
    Path or None
        Path to the written CSV file, or ``None`` if the DataFrame is empty.
    """
    if df.empty:
        logger.warning("Empty DataFrame – CSV not written.")
        return None

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"weather_data_{ts}.csv"

    df.to_csv(filepath, index=False, encoding="utf-8")
    logger.info("CSV saved: %s (%d rows)", filepath, len(df))
    return filepath


def load_to_sqlite(
    df: pd.DataFrame,
    db_path: str | Path = "data/weather.db",
) -> int:
    """Append the DataFrame to the ``weather_current`` table in SQLite.

    The table is created automatically if it does not exist.

    Parameters
    ----------
    df : pd.DataFrame
        Transformed weather data.
    db_path : str | Path
        Path to the SQLite database file.

    Returns
    -------
    int
        Number of rows inserted.
    """
    if df.empty:
        logger.warning("Empty DataFrame – nothing to insert into SQLite.")
        return 0

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            rows_before = conn.execute(
                "SELECT COUNT(*) FROM weather_current"
            ).fetchone()[0]

            df.to_sql("weather_current", conn, if_exists="append", index=False)

            rows_after = conn.execute(
                "SELECT COUNT(*) FROM weather_current"
            ).fetchone()[0]
            inserted = rows_after - rows_before

        logger.info(
            "SQLite: inserted %d rows into weather_current (total: %d)",
            inserted,
            rows_after,
        )
        return inserted

    except sqlite3.Error as exc:
        logger.error("SQLite error: %s", exc)
        raise
