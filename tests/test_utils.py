"""Unit tests for the utils module."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from weather_etl.utils import ensure_directory, load_config, setup_logging


# ── load_config ──────────────────────────────────────────────


def test_load_config_valid(tmp_path: Path) -> None:
    """A well-formed YAML file is loaded correctly."""
    cfg_file = tmp_path / "test.yaml"
    cfg_file.write_text(
        "api:\n  base_url: https://example.com\ncities:\n  - name: Test\n",
        encoding="utf-8",
    )
    config = load_config(cfg_file)

    assert config["api"]["base_url"] == "https://example.com"
    assert config["cities"][0]["name"] == "Test"


def test_load_config_missing_file() -> None:
    """A missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path.yaml")


# ── setup_logging ────────────────────────────────────────────


def test_setup_logging_creates_log_dir(tmp_path: Path) -> None:
    """Log directory is created automatically."""
    log_dir = tmp_path / "my_logs"
    logger = setup_logging(log_dir=log_dir)

    assert log_dir.exists()
    assert isinstance(logger, logging.Logger)

    # Cleanup handlers to avoid leaking in tests
    logger.handlers.clear()


# ── ensure_directory ─────────────────────────────────────────


def test_ensure_directory_creates(tmp_path: Path) -> None:
    """Missing directories (including parents) are created."""
    target = tmp_path / "a" / "b" / "c"
    result = ensure_directory(target)

    assert result.exists()
    assert result.is_dir()


def test_ensure_directory_existing(tmp_path: Path) -> None:
    """Calling on an existing directory is a no-op."""
    result = ensure_directory(tmp_path)

    assert result == tmp_path
