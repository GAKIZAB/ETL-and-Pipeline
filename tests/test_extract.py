"""Unit tests for the extract module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from weather_etl.extract import extract_all, fetch_weather

# ── Fixtures ─────────────────────────────────────────────────

PARIS = {"name": "Paris", "latitude": 48.8566, "longitude": 2.3522}
API_CONFIG = {
    "base_url": "https://api.open-meteo.com/v1/forecast",
    "timeout_seconds": 5,
    "max_retries": 2,
    "backoff_factor": 1,  # keep tests fast
    "current_weather": True,
}

SAMPLE_RESPONSE = {
    "latitude": 48.86,
    "longitude": 2.35,
    "current_weather": {
        "time": "2026-02-18T22:00",
        "temperature": 7.2,
        "windspeed": 12.5,
        "winddirection": 210,
        "weathercode": 3,
        "is_day": 0,
    },
}


# ── fetch_weather ────────────────────────────────────────────


@patch("weather_etl.extract.requests.get")
def test_fetch_weather_success(mock_get: MagicMock) -> None:
    """Successful API call returns parsed JSON."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_weather(PARIS, base_url=API_CONFIG["base_url"])

    assert result is not None
    assert result["current_weather"]["temperature"] == 7.2
    mock_get.assert_called_once()


@patch("weather_etl.extract.requests.get")
def test_fetch_weather_timeout_retries(mock_get: MagicMock) -> None:
    """Timeouts trigger retries up to max_retries."""
    mock_get.side_effect = requests.exceptions.Timeout("timeout")

    result = fetch_weather(
        PARIS,
        base_url=API_CONFIG["base_url"],
        max_retries=2,
        backoff_factor=1,
    )

    assert result is None
    assert mock_get.call_count == 2


@patch("weather_etl.extract.requests.get")
def test_fetch_weather_http_error(mock_get: MagicMock) -> None:
    """Non-200 responses trigger retries."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
    mock_get.return_value = mock_resp

    result = fetch_weather(
        PARIS,
        base_url=API_CONFIG["base_url"],
        max_retries=2,
        backoff_factor=1,
    )

    assert result is None
    assert mock_get.call_count == 2


@patch("weather_etl.extract.requests.get")
def test_fetch_weather_unrecoverable(mock_get: MagicMock) -> None:
    """Unrecoverable errors do not retry."""
    mock_get.side_effect = requests.exceptions.RequestException("fatal")

    result = fetch_weather(
        PARIS,
        base_url=API_CONFIG["base_url"],
        max_retries=3,
        backoff_factor=1,
    )

    assert result is None
    assert mock_get.call_count == 1


# ── extract_all ──────────────────────────────────────────────


@patch("weather_etl.extract.fetch_weather")
def test_extract_all_partial_failure(mock_fetch: MagicMock) -> None:
    """Pipeline continues when some cities fail."""
    mock_fetch.side_effect = [SAMPLE_RESPONSE, None]

    cities = [PARIS, {"name": "Bad", "latitude": 0, "longitude": 0}]
    results = extract_all(cities, API_CONFIG)

    assert len(results) == 1
    assert results[0]["city"] == "Paris"
