#!/usr/bin/env python3
"""
main.py – Entry point for the Weather ETL pipeline.

Usage
-----
    # One-shot run
    python main.py

    # Scheduled (runs every N minutes as defined in config)
    python main.py --schedule
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import NoReturn

import schedule as schedule_lib  # 'schedule' library

from weather_etl.extract import extract_all
from weather_etl.load import load_to_csv, load_to_sqlite
from weather_etl.transform import transform
from weather_etl.utils import load_config, setup_logging


def run_pipeline(config_path: str | None = None) -> None:
    """Execute a single Extract → Transform → Load cycle.

    Parameters
    ----------
    config_path : str | None
        Optional explicit path to the YAML configuration file.
    """
    # ── Configuration & logging ──────────────────────────────
    config = load_config(config_path)
    log_dir = config.get("paths", {}).get("log_dir", "logs")
    logger = setup_logging(log_dir=log_dir)

    logger.info("=" * 60)
    logger.info("Weather ETL pipeline – run started")
    logger.info("=" * 60)

    cities = config.get("cities", [])
    api_config = config.get("api", {})
    data_dir = config.get("paths", {}).get("data_dir", "data")
    db_path = config.get("paths", {}).get("database", "data/weather.db")

    if not cities:
        logger.error("No cities defined in configuration – aborting.")
        return

    # ── Extract ──────────────────────────────────────────────
    logger.info("PHASE 1 / 3 — Extract")
    raw_records = extract_all(cities, api_config)

    if not raw_records:
        logger.warning("No data extracted – pipeline ending early.")
        return

    # ── Transform ────────────────────────────────────────────
    logger.info("PHASE 2 / 3 — Transform")
    df = transform(raw_records)

    if df.empty:
        logger.warning("Transformation resulted in empty data – skipping load.")
        return

    # ── Load ─────────────────────────────────────────────────
    logger.info("PHASE 3 / 3 — Load")
    csv_path = load_to_csv(df, data_dir=data_dir)
    rows_inserted = load_to_sqlite(df, db_path=db_path)

    logger.info("Pipeline run complete.  CSV → %s | SQLite rows inserted → %d", csv_path, rows_inserted)
    logger.info("=" * 60)


def run_scheduled(config_path: str | None = None) -> NoReturn:
    """Run the pipeline on a recurring schedule defined in the config.

    Parameters
    ----------
    config_path : str | None
        Optional explicit path to the YAML configuration file.
    """
    config = load_config(config_path)
    interval = config.get("schedule", {}).get("interval_minutes", 60)
    logger = setup_logging(log_dir=config.get("paths", {}).get("log_dir", "logs"))

    logger.info("Scheduler started – running every %d minute(s).", interval)

    # Run immediately on start, then schedule subsequent runs
    run_pipeline(config_path)

    schedule_lib.every(interval).minutes.do(run_pipeline, config_path)

    while True:
        schedule_lib.run_pending()
        time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────


def main() -> None:
    """Parse CLI arguments and launch the appropriate mode."""
    parser = argparse.ArgumentParser(
        description="Weather ETL Pipeline – Open-Meteo API",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to the YAML configuration file (default: config/cities.yaml).",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run the pipeline on a recurring schedule.",
    )

    args = parser.parse_args()

    if args.schedule:
        run_scheduled(args.config)
    else:
        run_pipeline(args.config)


if __name__ == "__main__":
    main()
