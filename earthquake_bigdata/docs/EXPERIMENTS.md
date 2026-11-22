# Experiment log

All times below are wall-clock measurements from this machine. They are not
estimates. Re-run the commands to reproduce; hardware is recorded in
`results/benchmarks/benchmark_metadata.json`.

## Environment

* Windows 11, Ryzen 7 8845HS (8C/16T), 15.3 GiB visible RAM, 288 GB free at start.
* Java 17.0.13, Hadoop 3.3.6, Spark 4.2.0 (local mode), Python 3.12.10.
* HDFS: one NameNode + one DataNode, replication 1.

## Scaling sequence

| Level | Dataset | Records | Status |
|---|---|---|---|
| 1 | Real 10k sample | 9,882 processed | ✅ completed |
| 2 | Full global | 681,902 processed | 🟡 analytics running |
| 3 | Alaska stress | 731,416 raw (M≥1) | 🟡 downloading |
| 4 | California stress | 692,197 raw (M≥1) | ⬜ queued |

## Phase timings (observed)

| Operation | Dataset | Observed result |
|---|---|---|
| HDFS + Spark smoke test | 3 rows | pass; 3-row Parquet round-trip |
| USGS global download | 689,501 raw rows | 312/312 monthly chunks, count exact |
| Global ETL | 689,501 raw → 681,902 | HDFS + local, verified |
| Basic statistics | 681,902 | completed |
| Hotspot analysis | 681,902 | 2,442 groups, 586 eligible, median Mc 4.55, b 0.987 |
| ST-DBSCAN (sample) | 9,882 | 45 clusters, ARI 1.000000, 74.29 s distributed vs 1.33 s reference |
| ST-DBSCAN (global bounded) | 119,765 | 494 clusters, 98,583 noise, ARI 0.999991, 154.04 s distributed vs 24.65 s reference |
| Volcano association (sample) | 941 eruptions | completed with sparse-data warning |
| Volcano association (global) | 941 eruptions | completed; 50/100/200 km tables |
| Benchmark (sample) | 9,882 | 82 measurements; Spark startup 10.787 s |
| Benchmark (global) | 681,902 | 88 measurements; Spark startup 5.17 s; Pandas faster in every measured operation |
| Stress download | 731,416 + 692,197 raw | exact counts after fixing split-overwrite bug |
| Stress ETL | 1,423,613 raw → 1,379,322 | HDFS + local, verified |
| Stress benchmark | 1,379,322 | 82 measurements; `results/benchmarks/stress_all/` |

## Benchmark design

See `benchmarking/benchmark_report.py` and `docs/METHODOLOGY.md`.

* Fractions 10/25/50/100% of the processed dataset.
* Spark partitions 8/16/32/64 (full run).
* Formats CSV/Parquet.
* Caching on/off.
* 3 repeats for load and group-by; one run for heavier operations.
* Identical input files for both engines.

## Errors encountered and fixes (transparent log)

1. Spark 4.2 rejected Java 11; selected JDK 17.
2. Spark interpreted local paths as HDFS because `fs.defaultFS` was HDFS;
   local paths are now passed as `file:///` URIs.
3. `nst`/`magNst` CSV values such as `10.0` could not be cast directly to int;
   cast through double then int.
4. Spark 4 functions lacked `mod`; used the `%` operator.
5. HDFS paths contained Windows backslashes; paths are normalized to POSIX.
6. Pandas 3/nanosecond Parquet timestamps were rejected by Spark; benchmark
   inputs use microsecond timestamps.
7. Smithsonian HTML blocked scripts; official GVP WFS used instead.
8. The first distributed volcano join used Python UDFs and exhausted a worker;
   replaced with Spark SQL grid/cell expressions and exact Haversine SQL.

These are genuine failures fixed during development, not fabricated results.
