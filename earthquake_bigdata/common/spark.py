"""Spark session construction with Windows/Hadoop environment handling."""
from __future__ import annotations

import glob
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.config import load_config
from common.logging_utils import get_logger

log = get_logger("spark")

_SPARK_MIN_JAVA_MAJOR = 17


def _java_major(java_exe: Path) -> Optional[int]:
    try:
        proc = subprocess.run(
            [str(java_exe), "-version"],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        text = (proc.stderr or "") + (proc.stdout or "")
        match = re.search(r'version\s+"(\d+)(?:\.(\d+))?', text)
        if not match:
            return None
        major = int(match.group(1))
        if major == 1 and match.group(2):
            major = int(match.group(2))
        return major
    except Exception:
        return None


def find_java_home(cfg: Optional[Dict[str, Any]] = None) -> Path:
    """Find a Java runtime compatible with the installed PySpark (>=17)."""
    cfg = cfg or load_config()
    candidates: List[Path] = []

    def add(value: Optional[str]) -> None:
        if value:
            candidates.append(Path(value))

    add(os.environ.get("SPARK_JAVA_HOME"))
    if os.name == "nt":
        add(cfg.get("storage", {}).get("java_home_windows"))
    add(os.environ.get("JAVA_HOME"))
    if os.name == "nt":
        for pattern in (
            r"C:\Program Files\Eclipse Adoptium\jdk-17*",
            r"C:\Program Files\Eclipse Adoptium\jdk-21*",
            r"C:\Program Files\Java\jdk-17*",
            r"C:\Program Files\Java\jdk-21*",
        ):
            candidates.extend(Path(p) for p in glob.glob(pattern))
    else:
        for pattern in ("/usr/lib/jvm/java-17*", "/usr/lib/jvm/java-21*", "/opt/java/*"):
            candidates.extend(Path(p) for p in glob.glob(pattern))

    seen = set()
    for home in candidates:
        key = str(home).lower()
        if key in seen:
            continue
        seen.add(key)
        exe = home / "bin" / ("java.exe" if os.name == "nt" else "java")
        if not exe.exists():
            continue
        major = _java_major(exe)
        if major is not None and major >= _SPARK_MIN_JAVA_MAJOR:
            return home

    raise RuntimeError(
        "No Java 17+ runtime found. PySpark 4.2 requires Java 17 or newer. "
        "Set SPARK_JAVA_HOME to a JDK 17+ installation."
    )


def configure_native_environment(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Set JAVA_HOME / HADOOP_HOME / PATH in-process before Spark starts."""
    cfg = cfg or load_config()
    java_home = find_java_home(cfg)
    os.environ["JAVA_HOME"] = str(java_home)
    path_parts = [str(java_home / "bin")]

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home and os.name == "nt":
        hadoop_home = cfg.get("storage", {}).get("hadoop_home_windows")
    if hadoop_home:
        hh = Path(hadoop_home)
        if hh.exists():
            os.environ["HADOOP_HOME"] = str(hh)
            path_parts.append(str(hh / "bin"))
            if os.name == "nt":
                winutils = hh / "bin" / "winutils.exe"
                if winutils.exists():
                    os.environ.setdefault("HADOOP_OPTS", "")
    path_parts.append(os.environ.get("PATH", ""))
    os.environ["PATH"] = os.pathsep.join(path_parts)
    return {"JAVA_HOME": os.environ["JAVA_HOME"], "HADOOP_HOME": os.environ.get("HADOOP_HOME", "")}


def create_spark_session(
    app_name: str = "earthquake_bigdata",
    cfg: Optional[Dict[str, Any]] = None,
    master: Optional[str] = None,
    extra_conf: Optional[Dict[str, str]] = None,
):
    """Create a SparkSession in local mode (or the configured master)."""
    cfg = cfg or load_config()
    env = configure_native_environment(cfg)
    spark_cfg = cfg.get("spark", {})
    master = master or spark_cfg.get("master", "local[6]")
    hdfs_uri = cfg.get("storage", {}).get("hdfs_uri", "hdfs://localhost:9000")

    builder = (
        _spark_builder()
        .master(master)
        .appName(app_name)
        .config("spark.driver.memory", str(spark_cfg.get("driver_memory", "5g")))
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 32)))
        .config("spark.ui.enabled", str(spark_cfg.get("ui_enabled", False)).lower())
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", str(spark_cfg.get("arrow_enabled", True)).lower())
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.parquet.compression.codec", cfg.get("storage", {}).get("parquet_compression", "snappy"))
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(str(spark_cfg.get("log_level", "WARN")))
    log.info("Spark %s started (master=%s, JAVA_HOME=%s, HADOOP_HOME=%s)",
             spark.version, master, env.get("JAVA_HOME"), env.get("HADOOP_HOME"))
    return spark


def _spark_builder():
    # Imported lazily so that non-Spark utilities do not require Java.
    from pyspark.sql import SparkSession
    return SparkSession.builder


def stop_spark(spark) -> None:
    if spark is None:
        return
    try:
        spark.stop()
    except Exception:
        pass
    # On Windows, winutils may print cleanup messages; suppress them.
    if os.name == "nt":
        sys.stdout.flush()
        sys.stderr.flush()
