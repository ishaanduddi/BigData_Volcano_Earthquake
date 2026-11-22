"""Phase 2 environment smoke test.

Checks Java/Spark/HDFS through the same helpers used by the pipeline, performs a
Parquet round-trip on HDFS and locally, and prints a machine-readable report.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import get_path, load_config
from common.logging_utils import setup_logging
from common.spark import configure_native_environment, create_spark_session, stop_spark
from storage.hdfs_manager import resolve_backend
from common.io import ensure_dir, utc_now_iso


def main() -> int:
    cfg = load_config()
    setup_logging(cfg)
    env = configure_native_environment(cfg)
    backend, client = resolve_backend(cfg)
    spark = create_spark_session("phase2-smoke", cfg, master="local[2]",
                                 extra_conf={"spark.driver.memory": "2g", "spark.ui.enabled": "false"})
    report = {
        "generated_at": utc_now_iso(),
        "backend": backend,
        "java_home": env.get("JAVA_HOME"),
        "hadoop_home": env.get("HADOOP_HOME"),
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "hdfs_uri": client.uri if client else None,
        "checks": {},
    }
    try:
        df = spark.createDataFrame([(1, "alpha"), (2, "beta"), (3, "gamma")], ["id", "value"])
        df = df.withColumn("id2", df["id"] * 2)
        if backend == "hdfs" and client is not None:
            uri = client.hdfs_path("/tmp/earthquake_bigdata_smoke")
            df.write.mode("overwrite").parquet(uri)
            count = spark.read.parquet(uri).count()
            report["checks"]["hdfs_parquet_roundtrip_rows"] = count
            report["checks"]["hdfs_parquet_roundtrip"] = count == 3
            client.delete("/tmp/earthquake_bigdata_smoke")
        local = get_path(cfg, "data_processed") / "_smoke_test.parquet"
        ensure_dir(local.parent)
        df.write.mode("overwrite").parquet(local.resolve().as_uri())
        count = spark.read.parquet(local.resolve().as_uri()).count()
        report["checks"]["local_parquet_roundtrip_rows"] = count
        report["checks"]["local_parquet_roundtrip"] = count == 3
        import shutil
        shutil.rmtree(local, ignore_errors=True)
        report["checks"]["spark_sql"] = spark.sql("SELECT 6 * 7 AS answer").first()["answer"] == 42
        report["ok"] = all(v for k, v in report["checks"].items() if isinstance(v, bool))
    finally:
        stop_spark(spark)
    print(json.dumps(report, indent=2))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
