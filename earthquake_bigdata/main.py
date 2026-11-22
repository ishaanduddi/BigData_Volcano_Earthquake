"""Project command-line entry point.

Examples
--------
python main.py env
python main.py download-sample
python main.py etl-sample
python main.py stats --dataset sample_10k
python main.py hotspots --dataset sample_10k
python main.py cluster --dataset sample_10k
python main.py volcano --dataset sample_10k
python main.py benchmark --dataset sample_10k
python main.py dashboard
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import ensure_project_dirs, load_config
from common.io import utc_now_iso
from common.logging_utils import setup_logging
from common.spark import configure_native_environment, create_spark_session, find_java_home, stop_spark
from storage.hdfs_manager import HDFSClient, resolve_backend


def cmd_env(args) -> None:
    import platform
    import psutil
    cfg = load_config()
    setup_logging(cfg)
    try:
        java = str(find_java_home(cfg))
    except Exception as exc:
        java = f"ERROR: {exc}"
    backend, client = resolve_backend(cfg)
    info = {
        "generated_at": utc_now_iso(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "java_home": java,
        "hadoop_home": cfg.get("storage", {}).get("hadoop_home_windows"),
        "hdfs_uri": cfg.get("storage", {}).get("hdfs_uri"),
        "storage_backend": backend,
        "hdfs_reachable": bool(client and client.available()),
        "cpu_logical": psutil.cpu_count(logical=True),
        "ram_total_gib": round(psutil.virtual_memory().total / (1024 ** 3), 2),
        "disk_free_gib": round(psutil.disk_usage(str(ROOT.anchor)).free / (1024 ** 3), 2),
        "config": str(cfg.get("config_path")),
    }
    print(json.dumps(info, indent=2))
    if args.smoke:
        subprocess.run([sys.executable, str(ROOT / "scripts" / "smoke_test.py")], check=False)


def cmd_download(args) -> None:
    from ingestion.download_earthquakes import main as eq_main
    from ingestion.download_volcanoes import main as vol_main
    if args.what == "volcanoes":
        sys.argv = ["download_volcanoes", "--sample"]
        vol_main()
    else:
        sys.argv = ["download_earthquakes", "--dataset", args.what]
        if args.force:
            sys.argv.append("--force")
        eq_main()


def cmd_validate(args) -> None:
    sys.argv = ["validate_downloads"] + (["--strict"] if args.strict else [])
    from ingestion.validate_downloads import main
    main()


def cmd_etl(args) -> None:
    cfg = load_config()
    setup_logging(cfg)
    if args.what == "volcanoes":
        from preprocessing.volcano_etl import run
        print(json.dumps(run(cfg), indent=2, default=str))
    else:
        from preprocessing.earthquake_etl import run
        ds = {"sample": "sample_10k", "global": "global", "stress": "stress_all"}.get(args.what, args.what)
        print(json.dumps(run(ds, cfg), indent=2, default=str))


def cmd_analytics(args) -> None:
    cfg = load_config()
    setup_logging(cfg)
    if args.command == "stats":
        from analytics.basic_statistics import run
        result = run(args.dataset, cfg)
    elif args.command == "hotspots":
        from analytics.hotspot_analysis import run
        result = run(args.dataset, cfg)
    elif args.command == "cluster":
        from analytics.st_dbscan import run
        result = run(args.dataset, cfg)
    elif args.command == "volcano":
        from analytics.volcano_association import run
        result = run(args.dataset, cfg)
    else:
        raise ValueError(args.command)
    print(json.dumps(result, indent=2, default=str))


def cmd_benchmark(args) -> None:
    from benchmarking.benchmark_report import run
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(args.dataset, cfg, args.repeats, args.quick, not args.no_clustering), indent=2, default=str))


def cmd_dashboard(args) -> None:
    cmd = [sys.executable, "-m", "streamlit", "run", str(ROOT / "dashboard" / "app.py"),
           "--server.port", str(args.port)]
    subprocess.run(cmd, cwd=str(ROOT))


def cmd_pipeline(args) -> None:
    """Sequential sample (or global) pipeline with explicit stage logging."""
    cfg = load_config()
    setup_logging(cfg)
    ensure_project_dirs(cfg)
    dataset = "sample_10k" if args.scope == "sample" else "global"
    stages = []
    if args.scope == "sample":
        stages.append(["download-sample"])
        stages.append(["download-volcanoes"])
        stages.append(["validate"])
        stages.append(["etl", "sample"])
        stages.append(["etl", "volcanoes"])
    else:
        stages.append(["download-global"])
        stages.append(["download-volcanoes"])
        stages.append(["validate"])
        stages.append(["etl", "global"])
        stages.append(["etl", "volcanoes"])
    stages += [
        ["stats", "--dataset", dataset],
        ["hotspots", "--dataset", dataset],
        ["cluster", "--dataset", dataset],
        ["volcano", "--dataset", dataset],
        ["benchmark", "--dataset", dataset],
    ]
    for stage in stages:
        print(f"\n=== {' '.join(stage)} ===", flush=True)
        code = subprocess.run([sys.executable, str(ROOT / "main.py"), *stage], cwd=str(ROOT)).returncode
        if code != 0:
            print(f"Stage failed: {' '.join(stage)}", file=sys.stderr)
            raise SystemExit(code)


def build_parser():
    parser = argparse.ArgumentParser(description="Global earthquake / volcanism Big Data project")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("env", help="print environment and HDFS status")
    p.add_argument("--smoke", action="store_true")
    p.set_defaults(func=cmd_env)

    p = sub.add_parser("hadoop-start")
    p.set_defaults(func=lambda a: subprocess.run([sys.executable, str(ROOT / "scripts" / "start_hadoop.py")], cwd=str(ROOT)))
    p = sub.add_parser("hadoop-stop")
    p.set_defaults(func=lambda a: subprocess.run([sys.executable, str(ROOT / "scripts" / "stop_hadoop.py")], cwd=str(ROOT)))

    p = sub.add_parser("download", help="download real data")
    p.add_argument("what", choices=["sample", "global", "stress_all", "volcanoes"])
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_download)
    p = sub.add_parser("download-sample"); p.set_defaults(func=lambda a: cmd_download(argparse.Namespace(what="sample", force=False)))
    p = sub.add_parser("download-global"); p.set_defaults(func=lambda a: cmd_download(argparse.Namespace(what="global", force=False)))
    p = sub.add_parser("download-stress"); p.set_defaults(func=lambda a: cmd_download(argparse.Namespace(what="stress_all", force=False)))
    p = sub.add_parser("download-volcanoes"); p.set_defaults(func=lambda a: cmd_download(argparse.Namespace(what="volcanoes", force=False)))

    p = sub.add_parser("validate")
    p.add_argument("--strict", action="store_true")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("etl")
    p.add_argument("what", choices=["sample", "global", "stress", "volcanoes"])
    p.set_defaults(func=cmd_etl)

    for name in ("stats", "hotspots", "cluster", "volcano"):
        p = sub.add_parser(name)
        p.add_argument("--dataset", default="sample_10k")
        p.set_defaults(func=cmd_analytics)

    p = sub.add_parser("benchmark")
    p.add_argument("--dataset", default="sample_10k")
    p.add_argument("--repeats", type=int, default=None)
    p.add_argument("--quick", action="store_true")
    p.add_argument("--no-clustering", action="store_true")
    p.set_defaults(func=cmd_benchmark)

    p = sub.add_parser("dashboard")
    p.add_argument("--port", type=int, default=8501)
    p.set_defaults(func=cmd_dashboard)

    p = sub.add_parser("pipeline")
    p.add_argument("--scope", choices=["sample", "global"], default="sample")
    p.set_defaults(func=cmd_pipeline)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
