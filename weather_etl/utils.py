"""
Utility helpers – configuration loading, logging setup, and shared constants.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict

import yaml


# ── Configuration ────────────────────────────────────────────


def load_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    """Load the YAML configuration file and return it as a dictionary.

    Resolution order for *config_path*:
      1. Explicit argument.
      2. ``WEATHER_ETL_CONFIG`` environment variable.
      3. ``config/cities.yaml`` relative to the project root.

    Parameters
    ----------
    config_path : str | Path | None
        Optional explicit path to the YAML configuration file.

    Returns
    -------
    dict
        Parsed configuration dictionary.

    Raises
    ------
    FileNotFoundError
        If the resolved configuration file does not exist.
    yaml.YAMLError
        If the file contains invalid YAML.
    """
    if config_path is None:
        config_path = os.getenv(
            "WEATHER_ETL_CONFIG",
            str(Path(__file__).resolve().parent.parent / "config" / "cities.yaml"),
        )

    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        config: Dict[str, Any] = yaml.safe_load(fh)

    return config


# ── Logging ──────────────────────────────────────────────────


def setup_logging(
    log_dir: str | Path = "logs",
    log_level: int = logging.INFO,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure and return the application-wide logger.

    Creates a rotating file handler **and** a stream (console) handler so that
    every message is visible both in the terminal and persisted on disk.

    Parameters
    ----------
    log_dir : str | Path
        Directory where log files are written.
    log_level : int
        Minimum logging level (default ``logging.INFO``).
    max_bytes : int
        Maximum size of a single log file before rotation (default 5 MB).
    backup_count : int
        Number of rotated log files to keep.

    Returns
    -------
    logging.Logger
        Configured root logger for the application.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("weather_etl")
    logger.setLevel(log_level)

    # Avoid duplicate handlers when the function is called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler
    file_handler = RotatingFileHandler(
        filename=log_dir / "weather_etl.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ── Helpers ──────────────────────────────────────────────────


def ensure_directory(path: str | Path) -> Path:
    """Create a directory (and parents) if it doesn't already exist.

    Parameters
    ----------
    path : str | Path
        Directory path to ensure.

    Returns
    -------
    Path
        The resolved ``Path`` object.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
