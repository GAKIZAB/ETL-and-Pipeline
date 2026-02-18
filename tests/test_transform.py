"""Unit tests for the transform module."""

from __future__ import annotations

import pandas as pd
import pytest

from weather_etl.transform import EXPECTED_COLUMNS, transform

# ── Fixtures ─────────────────────────────────────────────────

VALID_RECORD = {
    "city": "Paris",
    "raw": {
        "current_weather": {
            "time": "2026-02-18T22:00",
            "temperature": 7.2,
            "windspeed": 12.5,
            "winddirection": 210,
            "weathercode": 3,
            "is_day": 0,
        }
    },
}

RECORD_MISSING_CW = {
    "city": "Broken",
    "raw": {},
}


# ── Tests ────────────────────────────────────────────────────


def test_transform_single_valid_record() -> None:
    """A single valid record produces a 1-row DataFrame."""
    df = transform([VALID_RECORD])

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert list(df.columns) == EXPECTED_COLUMNS
    assert df.iloc[0]["city"] == "Paris"
    assert df.iloc[0]["temperature_c"] == 7.2


def test_transform_empty_list() -> None:
    """An empty input produces an empty DataFrame with correct columns."""
    df = transform([])

    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS


def test_transform_skips_bad_records() -> None:
    """Records without 'current_weather' are skipped gracefully."""
    df = transform([RECORD_MISSING_CW, VALID_RECORD])

    assert len(df) == 1
    assert df.iloc[0]["city"] == "Paris"


def test_transform_data_types() -> None:
    """Numeric columns have proper data types after transformation."""
    df = transform([VALID_RECORD])

    assert pd.api.types.is_float_dtype(df["temperature_c"])
    assert pd.api.types.is_float_dtype(df["windspeed_kmh"])
    assert pd.api.types.is_float_dtype(df["winddirection_deg"])


def test_transform_retrieval_timestamp_present() -> None:
    """Every row has a non-null retrieval_timestamp."""
    df = transform([VALID_RECORD])

    assert df["retrieval_timestamp"].notna().all()
