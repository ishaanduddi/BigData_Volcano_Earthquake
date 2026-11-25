# Demo guide for the teacher

This is a 10–15 minute walkthrough. The heavy jobs are **already executed**;
the demo shows the pipeline, the stored data, the analytical results and the
dashboard without waiting for a full re-run.

Project folder:

```
C:\ISHU\Education\Amrita\5th_Sem\BigData\Project\earthquake_bigdata
```

---

## Before the demo (2 minutes)

Open PowerShell:

```powershell
cd C:\ISHU\Education\Amrita\5th_Sem\BigData\Project\earthquake_bigdata
.\scripts\start_hadoop.ps1
```

This starts HDFS if needed and creates the project directories. Then start the
dashboard in a second terminal:

```powershell
cd C:\ISHU\Education\Amrita\5th_Sem\BigData\Project\earthquake_bigdata
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Open **http://localhost:8501** in the browser.

If the dashboard was already running from an earlier test, stop the old one
first:

```powershell
Get-NetTCPConnection -LocalPort 8501 -State Listen | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

---

## 1. Start with the problem and architecture (1 minute)

Open `README.md` and show the architecture diagram:

```
USGS API ─┐
          ├─ resumable Python ingestion → raw local + HDFS
GVP WFS ──┘
          → PySpark ETL → Parquet partitioned by year
          → Spark analytics: hotspots / ST-DBSCAN / volcano statistics
          → saved result tables → Streamlit dashboard
```

Say:

> “This is a Big Data pipeline. Raw data is stored in HDFS, cleaning and
> aggregation run in Spark, and the dashboard reads only saved results. Spark is
> running in local multi-threaded mode on this one machine, not a multi-node
> cluster.”

---

## 2. Show the real data size in HDFS (2 minutes)

In a new PowerShell window:

```powershell
$env:JAVA_HOME="C:\PROGRA~1\ECLIPS~1\JDK-17~1.11-"
$env:PATH="$env:JAVA_HOME\bin;C:\hadoop\hadoop-3.3.6\bin;$env:PATH"

hdfs dfs -ls /earthquake/raw/global | Select-Object -First 8
hdfs dfs -du -s /earthquake/raw/global /earthquake/processed/global
hdfs dfs -ls /earthquake/processed/global | Select-Object -First 8
```

Say:

> “The raw layer preserves the original monthly USGS responses. The processed
> layer is Parquet partitioned by year. The global raw dataset is 312 monthly
> files; the processed dataset has 681,902 earthquake records.”
>
> “The USGS count endpoint reported 689,501 rows. 7,594 non-earthquake event
> types were removed by ETL; the count matches what actually exists.”

Optional: open the NameNode UI at **http://localhost:9870**.

---

## 3. Show the ingestion and ETL code (2 minutes)

Open:

- `ingestion/download_earthquakes.py`
- `preprocessing/earthquake_etl.py`

Point out:

1. **Resumable downloads**: each month has a manifest entry with count API,
   rows, SHA-256 and timestamp; rerunning skips validated months.
2. **Count validation**: monthly row count is checked against the USGS count
   endpoint.
3. **ETL filters**: genuine earthquakes only, valid coordinates, deduplication
   by event ID keeping the latest `updated` revision.
4. **Quality fields retained**: `gap`, `nst`, `dmin`, `rms`, horizontal/depth/
   magnitude errors.
5. **Parquet partitioned by year** with Snappy compression, written to HDFS
   and a local mirror.

You do **not** need to run the full download live.

---

## 4. Show the dashboard (5 minutes)

### Page 1 — Overview

Show the main numbers:

- 681,902 earthquakes
- 1,214 volcanoes
- 2000-01-01 → 2025-12-31
- HDFS backend

Say:

> “The dashboard does not run Spark. It reads saved result tables, so it loads
> quickly during the demo.”

### Page 2 — Earthquake Analysis

Show yearly/monthly trends, magnitude distribution, depth distribution and the
epicentre map.

Say:

> “The map is a bounded dashboard sample for responsiveness; the statistics are
> computed from all 681,902 processed records.”

### Page 3 — Hotspot Analysis

This is **contribution N1**:

- Raw vs corrected density maps
- Mc values by 5° cell and time window
- Gutenberg–Richter b-values
- Growth comparison

Say:

> “Raw counts are biased by how well a region is monitored. We estimate the
> magnitude of completeness Mc for each cell and window, then count only events
> above Mc. 44 cells show apparent growth in raw counts that disappears after
> this correction.”

Mention the median: **Mc 4.55, b-value 0.987**.

### Page 4 — Space–Time Clustering

This is **contribution N2**:

- Cluster map, cluster sizes, noise
- ST-DBSCAN vs DBSCAN vs K-Means
- ARI against the single-machine reference
- Parameter sensitivity

Say:

> “ST-DBSCAN uses Haversine distance and a 7-day temporal window. The distributed
> version partitions the globe into cells with overlapping halo events, runs
> local DBSCAN inside Spark partitions, and merges cross-partition clusters with
> Union–Find. It is not independent DBSCAN per partition.”
>
> “On the 119,765-event bounded global run we got 494 clusters and ARI
> 0.999991 against the single-machine reference. On the 10k sample ARI was
> exactly 1.0. A few border/core-boundary assignments differ; that limitation is
> reported in the results.”

Point out Spark local mode again. The 119,765-event sample is used because of
the 16 GB RAM limit; this is documented.

### Page 5 — Volcano Analysis

Select a volcano from the dropdown (the ranking is sorted by pre-eruption rate
ratio).

Show:

- Volcano location and nearby earthquakes
- Historical eruption windows
- Baseline vs 90-day pre / 180-day post rates
- Rate ratio, CI, p/q values, permutation p-value
- Radius sensitivity 50/100/200 km

Say:

> “This is association analysis, not prediction. Each volcano gets its own
> five-year baseline, excluding its eruption windows. We report effect sizes,
> confidence intervals, Poisson/binomial tests, permutation tests and FDR
> correction. Sparse baselines are flagged.”

### Page 6 — Performance Benchmarking

Show the measured speed-ups, CSV vs Parquet, partitions and caching.

Say:

> “Pandas was faster than Spark for every measured operation at this data size
> on one machine. We report that honestly. Spark’s fixed startup and scheduling
> overhead dominate at 681k rows; the benchmark does not pretend Spark is
> faster.”

Key examples:

- Group-by: Pandas 0.034 s vs Spark 0.195 s
- Spatial join: Pandas 1.14 s vs Spark 8.04 s
- Spark startup: 5.17 s
- Partitions: 8 and 16 are better than 32 and 64 here

---

## 5. Show that the code is tested (1 minute)

```powershell
cd C:\ISHU\Education\Amrita\5th_Sem\BigData\Project\earthquake_bigdata
.\.venv\Scripts\python.exe -m pytest -q
```

Expected: **24 passed**. The tests cover Haversine distance, Mc/b-value,
ETL filtering/deduplication, ST-DBSCAN synthetic cases, boundary merging,
volcano rate ratios and FDR.

A quick subset for a live demo (a few seconds):

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_geo.py tests\test_magnitude_completeness.py -q
```

---

## 6. Key limitations to state clearly

- HDFS is a **single-node development cluster** with replication 1.
- Spark is **local[6]**, not a multi-machine cluster.
- The global ST-DBSCAN run uses a **119,765-event bounded sample** because of
  the 16 GB RAM limit; this is recorded in `results/clustering/metrics.json`.
- ARI 0.999991 is near-equivalence; labels are arbitrary and border points can
  differ. The 10k sample achieved ARI 1.0.
- Mc and b-values depend on method, bin width and reported magnitude scales.
- Volcano analysis is **observational association**, not prediction or proof of
  causation.
- The dashboard map is a bounded sample, not every point.

---

## 7. If the teacher asks “show me a run”

Use a fast, safe command that does not take 30 minutes:

```powershell
.\.venv\Scripts\python.exe main.py env --smoke
```

This verifies Java, Spark, HDFS, Parquet round-trip and Spark SQL, and prints
`"ok": true`.

Then show the full command list in `README.md` for the heavy pipeline:

```powershell
.\.venv\Scripts\python.exe main.py stats --dataset global
.\.venv\Scripts\python.exe main.py hotspots --dataset global
.\.venv\Scripts\python.exe main.py volcano --dataset global
```

Explain that those are the exact commands used to produce the saved results.

---

## 30-second summary to end the demo

> “The project downloads and validates real USGS and Smithsonian data, stores raw
> and Parquet layers in HDFS, performs Spark ETL and analytics, implements a
> custom distributed ST-DBSCAN with boundary merging and validation, tests
> volcano–earthquake associations with volcano-specific baselines, benchmarks
> Pandas vs Spark honestly, and presents all results in a Streamlit dashboard.
> It is reproducible, tested, and the limitations are documented.”
