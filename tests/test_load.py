"""Unit tests for the load module."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from weather_etl.load import load_to_csv, load_to_sqlite

# ── Fixtures ─────────────────────────────────────────────────


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """Return a small DataFrame that mirrors the pipeline output."""
    return pd.DataFrame(
        {
            "city": ["Paris", "Lyon"],
            "timestamp": ["2026-02-18T22:00", "2026-02-18T22:00"],
            "temperature_c": [7.2, 5.8],
            "windspeed_kmh": [12.5, 8.3],
            "winddirection_deg": [210.0, 180.0],
            "weathercode": [3, 1],
            "is_day": [0, 0],
            "retrieval_timestamp": [
                "2026-02-18T22:01:00+00:00",
                "2026-02-18T22:01:00+00:00",
            ],
        }
    )


# ── CSV tests ────────────────────────────────────────────────


def test_load_to_csv_creates_file(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    """CSV file is created in the specified directory."""
    result = load_to_csv(sample_df, data_dir=tmp_path)

    assert result is not None
    assert result.exists()
    assert result.suffix == ".csv"

    loaded = pd.read_csv(result)
    assert len(loaded) == 2


def test_load_to_csv_empty_df(tmp_path: Path) -> None:
    """Empty DataFrame produces no CSV file."""
    empty = pd.DataFrame()
    result = load_to_csv(empty, data_dir=tmp_path)

    assert result is None


# ── SQLite tests ─────────────────────────────────────────────


def test_load_to_sqlite_creates_table(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    """Data is inserted and the table is created automatically."""
    db = tmp_path / "test.db"
    inserted = load_to_sqlite(sample_df, db_path=db)

    assert inserted == 2

    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM weather_current").fetchone()[0]
    assert rows == 2


def test_load_to_sqlite_appends(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    """Subsequent loads append rather than overwrite."""
    db = tmp_path / "test.db"
    load_to_sqlite(sample_df, db_path=db)
    load_to_sqlite(sample_df, db_path=db)

    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM weather_current").fetchone()[0]
    assert rows == 4


def test_load_to_sqlite_empty_df(tmp_path: Path) -> None:
    """Empty DataFrame inserts nothing."""
    db = tmp_path / "test.db"
    result = load_to_sqlite(pd.DataFrame(), db_path=db)

    assert result == 0
