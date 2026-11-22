"""Configuration loading and path resolution.

All modules import configuration through this module.  Paths in config.yaml are
relative to the project root and are resolved once here so that scripts can be
executed from any working directory.
"""
from __future__ import annotations

import copy
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _resolve_paths(cfg: Dict[str, Any]) -> None:
    resolved: Dict[str, Path] = {}
    for key, value in (cfg.get("paths") or {}).items():
        p = Path(value)
        resolved[key] = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
    cfg["resolved_paths"] = resolved
    cfg["project_root"] = PROJECT_ROOT
    cfg["config_path"] = Path(os.environ.get("EARTHQUAKE_CONFIG", DEFAULT_CONFIG)).resolve()


@lru_cache(maxsize=8)
def _load_cached(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    _resolve_paths(cfg)
    return cfg


def load_config(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load config.yaml, applying safe environment-variable overrides."""
    cfg_path = Path(path) if path else Path(os.environ.get("EARTHQUAKE_CONFIG", DEFAULT_CONFIG))
    cfg = copy.deepcopy(_load_cached(str(cfg_path.resolve())))

    env_overrides: Dict[str, Any] = {}
    if os.environ.get("EARTHQUAKE_STORAGE_BACKEND"):
        env_overrides.setdefault("storage", {})["backend"] = os.environ["EARTHQUAKE_STORAGE_BACKEND"]
    if os.environ.get("EARTHQUAKE_SPARK_MASTER"):
        env_overrides.setdefault("spark", {})["master"] = os.environ["EARTHQUAKE_SPARK_MASTER"]
    if os.environ.get("EARTHQUAKE_DRIVER_MEMORY"):
        env_overrides.setdefault("spark", {})["driver_memory"] = os.environ["EARTHQUAKE_DRIVER_MEMORY"]
    if env_overrides:
        cfg = _deep_merge(cfg, env_overrides)
        _resolve_paths(cfg)
    return cfg


def get_path(cfg: Dict[str, Any], key: str) -> Path:
    """Return an absolute local path from cfg['paths']."""
    try:
        return Path(cfg["resolved_paths"][key])
    except KeyError as exc:
        raise KeyError(f"Unknown configured path '{key}'") from exc


def ensure_project_dirs(cfg: Dict[str, Any]) -> None:
    """Create local data/results/log directories if they do not exist."""
    for path in cfg["resolved_paths"].values():
        Path(path).mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)


def get(cfg: Dict[str, Any], dotted: str, default: Any = None) -> Any:
    """Read a nested value with a dotted key, e.g. get(cfg, 'usgs.min_magnitude')."""
    node: Any = cfg
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node
