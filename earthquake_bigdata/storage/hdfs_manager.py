"""HDFS access through the Hadoop CLI.

This wrapper never pretends that local storage is HDFS.  When HDFS is
unavailable the caller receives an explicit "local" backend and a warning is
logged; all output metadata records which backend was actually used.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.config import load_config
from common.io import ensure_dir
from common.logging_utils import get_logger
from common.spark import configure_native_environment

log = get_logger("storage.hdfs")


class HDFSError(RuntimeError):
    pass


class HDFSClient:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or load_config()
        configure_native_environment(self.cfg)
        self.uri = self.cfg.get("storage", {}).get("hdfs_uri", "hdfs://localhost:9000").rstrip("/")
        self.timeout = int(self.cfg.get("storage", {}).get("hdfs_cli_timeout_seconds", 120))
        if os.name == "nt":
            hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop\hadoop-3.3.6")
            self.hdfs = str(Path(hadoop_home) / "bin" / "hdfs.cmd")
        else:
            self.hdfs = shutil.which("hdfs") or "hdfs"

    def _run(self, args: List[str], timeout: Optional[int] = None, check: bool = True):
        cmd = [self.hdfs, "dfs", *args]
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout or self.timeout,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        # Hadoop's Windows launcher can print "SUCCESS: ... terminated" to stderr
        # even on success; judge by return code and explicit error markers.
        err = proc.stderr or ""
        hard_error = any(
            marker in err
            for marker in (
                "Exception",
                "Error:",
                "Cannot create directory",
                "No such file or directory",
                "Call From",
                "Connection refused",
                "Safe mode is ON",
            )
        )
        if check and (proc.returncode != 0 or hard_error):
            raise HDFSError(f"HDFS command failed ({proc.returncode}): {' '.join(cmd)}\n{err[-3000:]}")
        return proc

    def available(self) -> bool:
        try:
            proc = self._run(["-ls", "/"], timeout=30, check=False)
            return proc.returncode == 0 and "Found" in (proc.stdout or "") or proc.returncode == 0
        except Exception as exc:
            log.warning("HDFS availability check failed: %s", exc)
            return False

    def mkdir(self, path: str) -> None:
        self._run(["-mkdir", "-p", self.hdfs_path(path)])

    def exists(self, path: str) -> bool:
        proc = self._run(["-test", "-e", self.hdfs_path(path)], check=False)
        return proc.returncode == 0

    def ls(self, path: str) -> str:
        return self._run(["-ls", self.hdfs_path(path)]).stdout

    def du(self, path: str) -> str:
        return self._run(["-du", "-s", self.hdfs_path(path)]).stdout

    def count(self, path: str) -> str:
        return self._run(["-count", self.hdfs_path(path)]).stdout

    def put_file(self, local: str | Path, remote: str, overwrite: bool = True) -> None:
        local = Path(local)
        if not local.exists():
            raise FileNotFoundError(local)
        parent = str(Path(remote).parent).replace("\\", "/")
        self.mkdir(parent)
        args = ["-put"]
        if overwrite:
            args.append("-f")
        args.extend([str(local), self.hdfs_path(remote)])
        self._run(args)

    def get_dir(self, remote: str, local: str | Path) -> None:
        ensure_dir(local)
        self._run(["-get", "-f", self.hdfs_path(remote), str(local)])

    def put_dir(self, local: str | Path, remote_parent: str, overwrite: bool = True) -> None:
        """Upload a local directory into an HDFS parent directory in one CLI call."""
        local = Path(local)
        if not local.exists():
            raise FileNotFoundError(local)
        self.mkdir(remote_parent)
        args = ["-put"]
        if overwrite:
            args.append("-f")
        args.extend([str(local), self.hdfs_path(remote_parent)])
        self._run(args)

    def delete(self, path: str, recursive: bool = True) -> None:
        args = ["-rm", "-r", "-f"] if recursive else ["-rm", "-f"]
        args.append(self.hdfs_path(path))
        self._run(args, check=False)

    def hdfs_path(self, path: str) -> str:
        if path.startswith("hdfs://"):
            return path
        p = "/" + str(path).replace("\\", "/").lstrip("/")
        return f"{self.uri}{p}"


def resolve_backend(cfg: Optional[Dict[str, Any]] = None) -> tuple[str, Optional[HDFSClient]]:
    """Resolve storage backend exactly as configured; never silently pretend."""
    cfg = cfg or load_config()
    requested = str(cfg.get("storage", {}).get("backend", "auto")).lower()
    if requested == "local":
        log.info("Storage backend forced to LOCAL by configuration.")
        return "local", None
    client = HDFSClient(cfg)
    if client.available():
        log.info("HDFS is available at %s; using HDFS as primary storage.", client.uri)
        return "hdfs", client
    if requested == "hdfs":
        raise HDFSError(
            f"HDFS backend requested but NameNode is not reachable at {client.uri}. "
            "Start HDFS with scripts/start_hadoop.ps1 or set storage.backend=local."
        )
    log.warning(
        "HDFS is not reachable at %s. Falling back to LOCAL processed storage. "
        "Results will be labelled backend=local; local storage is not HDFS.",
        client.uri,
    )
    return "local", None


def create_hdfs_layout(client: Optional[HDFSClient], cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    if client is None:
        return []
    cfg = cfg or load_config()
    layout = cfg.get("storage", {}).get("hdfs_layout", {})
    created = []
    for _name, path in layout.items():
        client.mkdir(path)
        created.append(path)
    log.info("Ensured HDFS layout: %s", ", ".join(created))
    return created
