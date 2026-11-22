"""Stop the development HDFS services."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import load_config
from common.spark import configure_native_environment


def main() -> int:
    cfg = load_config()
    configure_native_environment(cfg)
    hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop\hadoop-3.3.6"))
    if os.name == "nt":
        script = hadoop_home / "sbin" / "stop-dfs.cmd"
        if script.exists():
            subprocess.run([str(script)], timeout=120, creationflags=subprocess.CREATE_NO_WINDOW)
            print("Issued stop-dfs.cmd")
            return 0
        hdfs = str(hadoop_home / "bin" / "hdfs.cmd")
    else:
        hdfs = "hdfs"
    for daemon in ("datanode", "namenode"):
        subprocess.run([hdfs, "--daemon", "stop", daemon], timeout=120,
                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    print("Stop commands issued.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
