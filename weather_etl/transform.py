"""
Transform module – converts raw API responses into a clean pandas DataFrame.

Output columns:
  city, timestamp, temperature_c, windspeed_kmh, winddirection_deg,
  weathercode, is_day, retrieval_timestamp
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger("weather_etl.transform")

# Columns expected in the final DataFrame
EXPECTED_COLUMNS: List[str] = [
    "city",
    "timestamp",
    "temperature_c",
    "windspeed_kmh",
    "winddirection_deg",
    "weathercode",
    "is_day",
    "retrieval_timestamp",
]


def _parse_single(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a single city's raw API response into a flat dictionary.

    Parameters
    ----------
    record : dict
        Must contain ``city`` (str) and ``raw`` (dict with ``current_weather``).

    Returns
    -------
    dict or None
        Flat dict ready for DataFrame construction, or ``None`` on failure.
    """
    city_name = record.get("city", "unknown")
    raw = record.get("raw", {})
    current = raw.get("current_weather")

    if current is None:
        logger.warning("No 'current_weather' block for %s – skipping.", city_name)
        return None

    try:
        parsed: Dict[str, Any] = {
            "city": city_name,
            "timestamp": current.get("time"),
            "temperature_c": current.get("temperature"),
            "windspeed_kmh": current.get("windspeed"),
            "winddirection_deg": current.get("winddirection"),
            "weathercode": current.get("weathercode"),
            "is_day": current.get("is_day"),
            "retrieval_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return parsed

    except (KeyError, TypeError) as exc:
        logger.error("Error parsing data for %s: %s", city_name, exc)
        return None


def transform(raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transform a list of raw extraction records into a structured DataFrame.

    Parameters
    ----------
    raw_records : list[dict]
        Each element has ``city`` and ``raw`` keys, as returned by
        :func:`weather_etl.extract.extract_all`.

    Returns
    -------
    pd.DataFrame
        Structured DataFrame with columns defined in ``EXPECTED_COLUMNS``.
        May be empty if every record failed to parse.
    """
    rows: List[Dict[str, Any]] = []
    for record in raw_records:
        parsed = _parse_single(record)
        if parsed is not None:
            rows.append(parsed)

    if not rows:
        logger.warning("Transform produced an empty DataFrame.")
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    df = pd.DataFrame(rows, columns=EXPECTED_COLUMNS)

    # Ensure proper data types
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["windspeed_kmh"] = pd.to_numeric(df["windspeed_kmh"], errors="coerce")
    df["winddirection_deg"] = pd.to_numeric(df["winddirection_deg"], errors="coerce")
    df["weathercode"] = pd.to_numeric(df["weathercode"], errors="coerce").astype("Int64")
    df["is_day"] = pd.to_numeric(df["is_day"], errors="coerce").astype("Int64")

    logger.info("Transform complete: %d rows produced.", len(df))
    return df
