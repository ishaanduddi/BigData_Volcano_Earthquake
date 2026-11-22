"""Start the single-node Hadoop HDFS services on Windows/Linux.

This is a development convenience, not a production cluster manager.  It
starts one NameNode and one DataNode, leaves safe mode, and creates the project
HDFS layout.  The script is idempotent: already-running services are left
alone.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import load_config
from common.logging_utils import setup_logging
from common.spark import configure_native_environment
from storage.hdfs_manager import HDFSClient, create_hdfs_layout


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def main() -> int:
    cfg = load_config()
    setup_logging(cfg)
    configure_native_environment(cfg)
    hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop\hadoop-3.3.6"))
    if os.name == "nt":
        hdfs_cmd = hadoop_home / "bin" / "hdfs.cmd"
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        hdfs_cmd = Path("hdfs")
        flags = 0
    logdir = Path(os.environ.get("HADOOP_LOG_DIR", r"C:\hadoopdata\logs" if os.name == "nt" else "/tmp"))
    logdir.mkdir(parents=True, exist_ok=True)

    # NameNode RPC 9000, web UI 9870; DataNode 9864.
    if not port_open(9000):
        out = open(logdir / "namenode.log", "ab")
        err = open(logdir / "namenode.err", "ab")
        subprocess.Popen([str(hdfs_cmd), "namenode"], stdout=out, stderr=err,
                         stdin=subprocess.DEVNULL, creationflags=flags, close_fds=True)
        print("Started NameNode")
    else:
        print("NameNode already listening on 9000")
    time.sleep(4)
    if not port_open(9864):
        out = open(logdir / "datanode.log", "ab")
        err = open(logdir / "datanode.err", "ab")
        subprocess.Popen([str(hdfs_cmd), "datanode"], stdout=out, stderr=err,
                         stdin=subprocess.DEVNULL, creationflags=flags, close_fds=True)
        print("Started DataNode")
    else:
        print("DataNode already listening on 9864")

    client = HDFSClient(cfg)
    for _ in range(30):
        if client.available():
            break
        time.sleep(2)
    else:
        print("HDFS did not become reachable. Check logs in", logdir)
        return 1
    # Safe mode can remain on after an unclean shutdown.
    subprocess.run([client.hdfs, "dfsadmin", "-safemode", "leave"], capture_output=True, text=True,
                   timeout=60, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    create_hdfs_layout(client, cfg)
    print(f"HDFS ready at {client.uri}")
    print(client.ls("/"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
