# Environment Assessment

Date of inspection: **2026-11-19**. All facts below were measured on the
development machine, not assumed.

## Hardware

| Item | Measured value |
|---|---|
| CPU | AMD Ryzen 7 8845HS (8 cores / 16 logical processors, up to 3.8 GHz) |
| RAM | 16,050,548 KiB total visible (~15.3 GiB); ~4.6 GB free during inspection |
| Disk (C:) | 663.42 GB used, **288.23 GB free** |
| OS | Windows 11 Home Single Language, build 10.0.26300, 64-bit |
| Virtualisation | WSL2 available (Ubuntu-20.04 installed but stopped); Docker Desktop installed but stopped |

## Software found

| Component | Version / state | Notes |
|---|---|---|
| Python | 3.12.10 | Windows Store Python |
| pip | 26.2.1 (global) | Project venv has pip 25.0.1 |
| Java | Temurin JDK 11.0.31 (default `JAVA_HOME`), JDK 17.0.13 and JDK 23 also installed | **Spark 4.2 requires Java 17+**, so project scripts select JDK 17. |
| Hadoop | 3.3.6 at `C:\hadoop\hadoop-3.3.6` | `winutils.exe` present; `core-site.xml` uses `hdfs://localhost:9000`; NameNode/DataNode directories under `C:\hadoopdata`. |
| HDFS | Tested working | Started NameNode + one DataNode; `hdfs dfsadmin -report` reports ~951 GB configured capacity (host disk) and 288 GB present. A Spark Parquet round-trip through `hdfs://localhost:9000` succeeded. |
| PySpark | 4.2.0 | Spark smoke test: `local[2]`, 1,000 rows, Java 17.0.13. |
| Git | 2.51.0 | Repository initialised inside `earthquake_bigdata/`. |
| Docker | 29.5.2 | Not required; native Hadoop is used. |

## Python packages found globally

`pandas 3.0.6`, `numpy 2.5.3`, `scipy 1.18.1`, `scikit-learn 1.9.1`,
`pyspark 4.2.0`, `pyarrow 26.0.0`, `plotly 7.1.0`, `matplotlib 3.11.2`,
`seaborn 0.13.2`, `statsmodels` was not installed. Streamlit was not installed.

Because PySpark 4.2 explicitly warns that **pandas 3 is not fully supported**,
the project uses a virtual environment with `pandas==2.2.3` and
`numpy==2.2.6`. The venv is created with `--system-site-packages` so that the
already-installed PySpark/scipy stack is reused, while pandas/numpy are
shadowed by compatible versions.

## Recommended compatible stack

| Component | Recommended | Reason |
|---|---|---|
| Python | 3.12.x | Tested; all pinned wheels available. |
| Java | **Temurin JDK 17** | Required by Spark 4.2; Hadoop 3.3.6 runs on it. |
| Hadoop | 3.3.6 | Already installed and configured. |
| Spark / PySpark | 4.2.0 | Already installed; works with Java 17. |
| pandas | 2.2.3 | Avoids PySpark/pandas-3 incompatibility. |
| NumPy | 2.2.6 | Compatible with pandas 2.2.3 and SciPy 1.18. |
| Storage | HDFS primary + local mirror | HDFS is proven working; local mirror keeps the dashboard independent of HDFS being up. |
| Dashboard | Streamlit 1.49.1 + Plotly 7.1.0 | Interactive maps/charts without a paid map token. |

## Data source checks

* **USGS FDSN Event Web Service** (`https://earthquake.usgs.gov/fdsnws/event/1/`)
  responded with API version `2.8.4`.
* Actual count for global `M >= 2.5`, `2000-01-01` to `2025-12-31`:
  **689,501 events** (count endpoint, measured 2026-11-19).
* Actual counts for the stress-test boxes (M >= 1.0, same period):
  Alaska box **731,416**, California box **692,197**. These are approximate
  boxes and are used *only* for scalability tests.
* **Smithsonian GVP** public web pages return a bot-verification challenge
  (HTTP 403) to automated clients. The **official GVP GeoServer WFS**
  (`https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows`) is reachable
  and returns `numberMatched = 1214` Holocene volcanoes and
  `numberMatched = 11089` Holocene eruptions. This official WFS is used.

## Hardware / memory considerations

* Spark local mode is configured with `local[6]` and a 5 GB driver. The machine
  has 16 GB RAM; running the dashboard, browser and Spark simultaneously needs
  care. Full-data ETL and clustering are designed to be run separately.
* 8 physical cores / 16 threads: Spark partition counts are chosen in the
  8–64 range. Benchmarks use identical input data and the same worker count for
  fair comparisons.
* The host has 288 GB free, far above the estimated project footprint.
* HDFS has one DataNode and replication factor 1. This is a single-machine
  development cluster, **not** a fault-tolerant multi-node cluster.

## Estimated storage requirements

| Item | Estimate |
|---|---|
| Global raw monthly CSVs (689k events) | ~130–200 MB |
| Global processed Parquet (partitioned by year, Snappy) | ~60–120 MB |
| Alaska + California stress raw CSVs (~1.42M events) | ~300–450 MB |
| Stress processed Parquet | ~120–250 MB |
| Volcano/eruption raw + processed | < 20 MB |
| Hotspot/cluster/volcano result tables | < 500 MB |
| Benchmark intermediate copies (10/25/50/100%) | < 3 GB |
| Local mirror + HDFS copies + logs | < 8 GB total |
| **Recommended free space** | **20 GB** (288 GB available) |

## Risks and mitigations

1. **Windows Hadoop instability / safe mode after unclean shutdown.** The
   start script leaves safe mode and verifies HDFS. A local mirror is written
   so analysis is not blocked if HDFS is down; metadata labels the actual
   backend.
2. **Java version mismatch.** Default `JAVA_HOME` is Java 11, which Spark 4.2
   rejects. Project code automatically selects JDK 17 and documents it.
3. **Smithsonian bot protection.** The official WFS endpoint is used instead
   of scraping the website; no coordinates or eruptive dates are invented.
4. **Memory pressure.** Full ST-DBSCAN uses a bounded deterministic sample;
   the exact sample size is recorded. Spark driver memory is capped at 5 GB.
5. **USGS catalogue revisions.** Monthly counts can change between the count
   and download calls. The downloader validates row counts, records hashes and
   aborts on material mismatches. Duplicate IDs are reduced to the latest
   revision in ETL.
6. **Scientific interpretation.** Corrected rates depend on the maximum
   curvature Mc estimate and on reported magnitude scales; results are
   association/description, not prediction or causation.
7. **HDFS on Windows is development-only.** Local mode Spark is multi-threaded
   on one machine, not a real distributed cluster. This distinction is stated
   in the README, dashboard and benchmark report.
