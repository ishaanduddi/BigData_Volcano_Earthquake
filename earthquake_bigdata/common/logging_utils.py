"""Logging setup shared by all project entry points."""
from __future__ import annotations

import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from common.config import PROJECT_ROOT, ensure_project_dirs, load_config


def setup_logging(
    cfg: Optional[Dict[str, Any]] = None,
    name: str = "earthquake_bigdata",
    level: Optional[str] = None,
) -> logging.Logger:
    """Configure console + rotating-file logging from config/logging.yaml.

    Safe to call repeatedly; existing handlers are reconfigured rather than
    duplicated.
    """
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    log_cfg_path = PROJECT_ROOT / cfg.get("logging", {}).get("config", "config/logging.yaml")
    with open(log_cfg_path, "r", encoding="utf-8") as fh:
        log_cfg = yaml.safe_load(fh)
    # Make the file handler path absolute and ensure the directory exists.
    file_handler = log_cfg.get("handlers", {}).get("file", {})
    filename = Path(file_handler.get("filename", "logs/earthquake_bigdata.log"))
    if not filename.is_absolute():
        filename = PROJECT_ROOT / filename
    filename.parent.mkdir(parents=True, exist_ok=True)
    file_handler["filename"] = str(filename)
    logging.config.dictConfig(log_cfg)
    logger = logging.getLogger(name)
    if level:
        logger.setLevel(level.upper())
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return logger


def get_logger(module: str) -> logging.Logger:
    """Return a child logger without altering global logging configuration."""
    return logging.getLogger(f"earthquake_bigdata.{module}")
