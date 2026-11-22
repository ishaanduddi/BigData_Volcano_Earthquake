# Project Status

Last updated: 2026-11-19 (late evening)

## Phase tracker

| Phase | Scope | Status |
|---|---|---|
| 1 | Environment inspection and project skeleton | ✅ Complete |
| 2 | Python / Hadoop / HDFS / Spark configuration and smoke tests | ✅ Complete |
| 3 | Real 10k earthquake sample + volcano sample; validation | ✅ Complete |
| 4 | ETL, Parquet/HDFS storage, sample pipeline | ✅ Complete |
| 5 | Basic earthquake analytics | ✅ Complete (sample + full global) |
| 6 | Corrected hotspot analysis (Mc, b-value) | ✅ Complete (sample + full global) |
| 7 | Distributed ST-DBSCAN + synthetic/boundary validation | ✅ Complete (sample + 120k global sample) |
| 8 | Volcano–earthquake association | ✅ Complete (sample + full global) |
| 9 | Pandas vs Spark benchmarking | ✅ Complete (sample + stress + global) |
| 10 | Streamlit dashboard | ✅ Built and served; reads saved results |
| 11 | End-to-end sample integration | ✅ Complete |
| 12 | Full global dataset | ✅ Downloaded, validated, ETL, analytics complete |
| 13 | Alaska/California stress-test data | ✅ Downloaded, validated, ETL complete |
| 14 | Final benchmarks and results | ✅ Complete (global + stress) |
| 15 | Documentation and report material | ✅ Complete (README + docs: environment, datasets, methodology, architecture, experiments, results) |

## Phase 1–4

Environment: Windows 11, Ryzen 7 8845HS (8C/16T), 16 GB RAM, 288 GB free.
Python 3.12.10, PySpark 4.2.0, Hadoop 3.3.6, Java 17. HDFS and Spark Parquet
round-trip verified. Real 10k sample: 13 monthly files, 29,498 raw rows →
10,000-row stratified sample; 1,214 volcanoes + 11,089 eruptions. ETL sample:
10,000 → 9,882 rows; HDFS + local Parquet verified. See earlier detail in the
repository history and `results/metadata/`.

## Phase 5 — basic statistics

Global: **681,902** processed records, 2000-01-01T01:19:26Z →
2025-12-31T23:57:05Z, mean magnitude 3.811, median 4.0, max 9.1, mean depth
56.7 km, 1,407 occupied 5° cells, 22 magnitude types. Outputs in
`results/basic/`.

## Phase 6 — corrected hotspots

Global: 2,442 cell×window groups; 586 ranking-eligible; 494 excluded by the
dominant-scale policy; 1,699 insufficient sample. Median Mc **4.55**, median b
**0.987**. Growth comparison (198 cells): 96 growth persists, 53 decline
persists, **44 apparent growth disappears after Mc correction**, 5 declines
reverse. Outputs in `results/hotspots/`.

## Phase 7 — distributed ST-DBSCAN

* Synthetic tests: **10 passed**, including a cluster crossing a partition
  boundary.
* Sample 9,882 events: 45 clusters, 7,542 noise, **ARI = 1.000000** vs
  single-machine reference.
* Global bounded run: 119,765 events (deterministic 17.6% sample), 494 global
  clusters, 98,583 noise, 21,182 events in clusters. Distributed **154.04 s**,
  reference **24.65 s**, spatial DBSCAN 12.79 s, K-Means 1.00 s.
* ARI vs reference **0.999991** on all 119,765; core-point ARI **0.998728** on
  18,370 core points. A small number of border/core-boundary assignments differ;
  this is reported, not hidden. The 10k sample achieved exact ARI 1.0.
* F1 vs the simplified magnitude-window reference: **0.405** (precision 0.770,
  recall 0.274). The reference is not ground truth.
* The distributed merge is driver-side Union–Find over local cluster links.
* Outputs in `results/clustering/`.

## Phase 8 — volcano association

Global, 941 confirmed day/month-precision eruptions, 221 volcanoes, earthquakes
joined by exact Haversine after a Spark 5° grid join:

| Radius | Sufficient-baseline eruptions | Pairs | Median pre RR | Median post RR | Significant pre FDR | Significant post FDR |
|---:|---:|---:|---:|---:|---:|---:|
| 50 km | 413 | 270,820 | 1.147 | 0.877 | 47 | 45 |
| 100 km | 569 | 697,875 | 1.178 | 1.015 | 127 | 126 |
| 200 km | 626 | 2,242,340 | 1.119 | 1.078 | 235 | 258 |

These are association statistics with volcano-specific baselines, confidence
intervals, Poisson/binomial tests, permutation tests and BH FDR. They are **not**
eruption predictions or causal claims. Sparse baselines are flagged. Outputs in
`results/volcano/`.

## Phase 9 — benchmark

* Sample 10k: 82 measured runs; Pandas faster on every operation; Spark startup
  10.787 s. This is an actual result, displayed rather than hidden.
* Stress-test (1.38M rows): 82 measured runs; CSV/Parquet, 10/25/50/100%,
  partitions 8/16, caching; `results/benchmarks/stress_all/`.
* Global full matrix: **88 measurements** at 681,902 rows; 8/16/32/64
  partitions, CSV/Parquet, cached/uncached, bounded ST-DBSCAN. **Pandas was
  faster for every measured operation** on this single machine; for example
  group-by 0.0336 s vs 0.195 s, spatial join 1.14 s vs 8.04 s. This is displayed,
  not hidden.
* Stress-test matrix: 82 measurements at 1,379,322 rows.
* Spark runs in local[6] mode on one machine, not a multi-node cluster.

## Phase 10 — dashboard

Streamlit app served at `http://localhost:8501` (HTTP 200). All six pages were
opened and rendered against saved sample results. Pages read only saved files;
they do not execute Spark. Full global results are now available to the same
pages.

## Phase 11–13 — integration and scale

* Phase 11 sample end-to-end: passed (download → validate → ETL → basic stats →
  hotspots → ST-DBSCAN → volcano → benchmark → dashboard).
* Phase 12 full global: 312 monthly chunks, **689,501 raw rows exactly matching
  the USGS count**; ETL **681,902** final; raw HDFS upload 312 files; processed
  Parquet 52.9 MB HDFS / 53.3 MB local; all analytics executed.
* Phase 13 stress: Alaska **731,416** + California **692,197** raw rows
  (California 2019-07 was split into daily files because it exceeded the USGS
  20,000 limit; the initial split-overwrite bug was found and fixed). Combined
  ETL: **1,379,322** final rows; HDFS 93.1 MB / local 93.9 MB. Data are kept
  separate from global findings.

## Open issues / limitations

1. Global ST-DBSCAN uses a documented 119,765-event deterministic sample to fit
   16 GB RAM; it is not the full 681,902 events. The exact sample note is in
   `results/clustering/metrics.json`.
2. Distributed vs reference labels are not bit-identical on the large sample
   (ARI 0.999991). Border points can belong to more than one cluster; core ARI
   is 0.998728. The method's equivalence was exact on the 10k sample and
   synthetic boundary tests.
3. Mc and b-values depend on method/bin/sample choices and reported magnitude
   scales; the dominant-family rule is conservative but not a physical
   conversion.
4. Volcano baselines are sparse for many eruptions; wide CIs and FDR flags are
   reported, not hidden.
5. HDFS is a single-node Windows development cluster (replication 1); Spark is
   local mode.
6. Dashboard uses a bounded event map/dashboard sample, not every point.

## Next actions

1. Finish documentation/report material and a final consistency check of README,
   results and dashboard.
2. Run the full pytest suite once more (24 tests currently pass) and commit.
3. Optionally improve distributed/reference exactness at very high latitudes by
   increasing halo rings (memory/time trade-off).
