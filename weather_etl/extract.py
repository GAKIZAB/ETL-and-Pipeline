"""
Extract module – fetches current weather data from the Open-Meteo API.

Implements:
  • Per-city HTTP requests with configurable timeout.
  • Exponential back-off retry logic for transient failures.
  • Graceful handling of partial failures (one bad city ≠ pipeline abort).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("weather_etl.extract")


def fetch_weather(
    city: Dict[str, Any],
    base_url: str,
    timeout: int = 10,
    max_retries: int = 3,
    backoff_factor: int = 2,
    current_weather: bool = True,
) -> Optional[Dict[str, Any]]:
    """Fetch current weather for a single city from the Open-Meteo API.

    Parameters
    ----------
    city : dict
        Must contain keys ``name``, ``latitude``, and ``longitude``.
    base_url : str
        Open-Meteo forecast endpoint URL.
    timeout : int
        HTTP request timeout in seconds.
    max_retries : int
        How many times to retry on transient errors.
    backoff_factor : int
        Multiplier for exponential back-off between retries.
    current_weather : bool
        Whether to request the ``current_weather`` block from the API.

    Returns
    -------
    dict or None
        Raw JSON response as a dictionary, or ``None`` on failure.
    """
    params: Dict[str, Any] = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current_weather": str(current_weather).lower(),
    }

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Requesting weather for %s (attempt %d/%d)",
                city["name"],
                attempt,
                max_retries,
            )
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()

            data = response.json()
            logger.info("Successfully fetched weather for %s", city["name"])
            return data

        except requests.exceptions.Timeout:
            logger.warning(
                "Timeout fetching %s (attempt %d/%d)",
                city["name"],
                attempt,
                max_retries,
            )
        except requests.exceptions.HTTPError as exc:
            logger.warning(
                "HTTP error for %s: %s (attempt %d/%d)",
                city["name"],
                exc,
                attempt,
                max_retries,
            )
        except requests.exceptions.ConnectionError as exc:
            logger.warning(
                "Connection error for %s: %s (attempt %d/%d)",
                city["name"],
                exc,
                attempt,
                max_retries,
            )
        except requests.exceptions.RequestException as exc:
            logger.error(
                "Unrecoverable request error for %s: %s",
                city["name"],
                exc,
            )
            return None

        if attempt < max_retries:
            wait = backoff_factor ** attempt
            logger.info("Retrying in %d seconds …", wait)
            time.sleep(wait)

    logger.error(
        "All %d attempts failed for %s – skipping.",
        max_retries,
        city["name"],
    )
    return None


def extract_all(
    cities: List[Dict[str, Any]],
    api_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Fetch weather data for every city in the list.

    Partial failures are logged but do **not** abort the pipeline; results
    for the remaining cities are still collected.

    Parameters
    ----------
    cities : list[dict]
        Each dict must include ``name``, ``latitude``, ``longitude``.
    api_config : dict
        API-related settings loaded from ``config/cities.yaml``.

    Returns
    -------
    list[dict]
        A list of ``{"city": …, "raw": …}`` dicts for successful fetches.
    """
    results: List[Dict[str, Any]] = []
    for city in cities:
        raw = fetch_weather(
            city=city,
            base_url=api_config["base_url"],
            timeout=api_config.get("timeout_seconds", 10),
            max_retries=api_config.get("max_retries", 3),
            backoff_factor=api_config.get("backoff_factor", 2),
            current_weather=api_config.get("current_weather", True),
        )
        if raw is not None:
            results.append({"city": city["name"], "raw": raw})

    logger.info(
        "Extraction complete: %d/%d cities succeeded.",
        len(results),
        len(cities),
    )
    return results
