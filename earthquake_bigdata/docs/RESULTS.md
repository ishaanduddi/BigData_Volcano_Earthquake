# Results (actual executed outputs)

Every value below comes from a saved file under `results/`. Nothing is
hand-invented. Raw files, processed Parquet, logs and metadata record how the
numbers were produced.

## 1. Basic statistics — full global dataset

Source: `results/basic/summary.json` (681,902 processed records).

| Metric | Value |
|---|---|
| Records | 681,902 |
| Date range (UTC) | 2000-01-01T01:19:26Z → 2025-12-31T23:57:05Z |
| Mean magnitude | 3.811 |
| Median magnitude | 4.0 |
| Max magnitude | 9.1 |
| Mean depth | 56.7 km |
| Median depth | 20.35 km |
| Magnitude types | 22 |
| Networks | 22 |
| Occupied 5° cells | 1,407 |

The raw USGS count was 689,501; ETL removed 7,594 non-earthquake event types and
5 other invalid/out-of-scope rows. Report:
`results/metadata/preprocessing_earthquakes_global.json`.

## 2. Corrected hotspots — full global dataset

Source: `results/hotspots/summary.json`.

| Metric | Value |
|---|---|
| Processed records | 681,902 |
| Cell × window groups | 2,442 |
| Occupied 5° cells | 1,407 |
| Ranking-eligible groups | 586 |
| Excluded by dominant-scale policy | 494 |
| Insufficient sample | 1,699 |
| Median Mc | 4.55 |
| Median b-value | 0.987 |
| Growth-comparison cells | 198 |

Growth classes (first vs second window): `apparent_growth_persists` 96,
`decline_persists` 53, **`apparent_growth_disappears_after_correction` 44**,
`apparent_decline_reverses_after_correction` 5.

The 44 cells are the main empirical result of N1 on this dataset: raw counts
grew, but rates of events above the locally estimated Mc did not. This is a
detection-correction result, not proof of a physical change.

## 3. ST-DBSCAN — full-data bounded run

Source: `results/clustering/metrics.json`.

| Metric | Value |
|---|---|
| Evaluated sample | 119,765 events (deterministic 17.6% of 681,902) |
| Global clusters | 494 |
| Noise events | 98,583 |
| Events in clusters | 21,182 |
| Distributed runtime | 154.04 s |
| Single-machine reference runtime | 24.65 s |
| ARI (distributed vs reference) | 0.999991 |
| Core-point ARI | 0.998728 (18,370 core points) |
| F1 vs simplified window reference | 0.405 (precision 0.770, recall 0.274) |
| Spatial DBSCAN runtime | 12.79 s |
| K-Means runtime | 1.00 s |

The 10k sample had exact ARI 1.000000. On the larger sample a small number of
border/core-boundary assignments differ; this is reported rather than hidden.
The distributed run genuinely executes local clustering in Spark partitions and
merges cross-partition clusters with Union–Find.

## 4. Volcano association — full global

Source: `results/volcano/summary.json`.

| Radius | Sufficient eruptions | Pairs | Median pre RR | Median post RR | FDR-sig pre | FDR-sig post |
|---:|---:|---:|---:|---:|---:|---:|
| 50 km | 413 | 270,820 | 1.147 | 0.877 | 47 | 45 |
| 100 km | 569 | 697,875 | 1.178 | 1.015 | 127 | 126 |
| 200 km | 626 | 2,242,340 | 1.119 | 1.078 | 235 | 258 |

941 confirmed eruptions, 221 volcanoes. These are associations with
volcano-specific baselines and FDR-corrected tests. They are not eruption
predictions and do not establish causation. Sparse baselines are flagged.
Extreme single-eruption ratios (e.g. a volcano with only one sufficient
baseline) are displayed but should be read with the baseline counts and q-values.

## 5. Benchmark — full global matrix

Source: `results/benchmarks/global/`.

| Operation | Pandas 100% (s) | Spark 100% cached (s) | Pandas/Spark speed-up |
|---|---:|---:|---:|
| Group-by year | 0.0336 | 0.1951 | 0.172 |
| Per-grid b-value | 0.3249 | 10.4333 | 0.031 |
| Spatial join (100 km) | 1.1444 | 8.0417 | 0.142 |

On this 681,902-row single-machine setup **Pandas is faster for every measured
operation**; the numbers are shown as measured. Spark startup was 5.17 s.

CSV vs Parquet group-by: pandas CSV 0.0280 s, pandas Parquet 0.0336 s, Spark
CSV 0.6753 s, Spark Parquet 0.1951 s. Spark partitions (100%): 8 → 1.315 s,
16 → 1.281 s, 32 → 1.962 s, 64 → 3.413 s. Cached 0.195 s vs uncached 0.245 s.

Clustering overhead on 50,000 rows: Pandas reference 1.917 s, Spark distributed
35.729 s. This includes scheduling and the driver-side merge; it is not a
parallel-speed win at this scale.

The benchmark is intentionally honest: fixed Spark overhead dominates this data
size on one machine. The project does not claim Spark is faster.

## 6. Stress-test dataset (scalability only)

| Region | Raw rows | Processed after ETL |
|---|---:|---:|
| Alaska box | 731,416 | — |
| California box | 692,197 | — |
| Combined | 1,423,613 | **1,379,322** |

California 2019-07 contained 23,620 events, above the USGS 20,000 limit, and was
downloaded as 31 daily files. An initial split implementation overwrote parts;
validation caught the 17,483-row deficit, the bug was fixed, and the month was
re-downloaded to the exact count. Stress Parquet: 93.1 MB HDFS / 93.9 MB local.
Stress benchmark (82 measurements) is in `results/benchmarks/stress_all/`.
These records are **not** mixed into global scientific findings.

## 7. Sample-only validation results

The 10k sample is used to validate the pipeline, not to make global claims:

* 9,882 processed records from 13 months.
* Hotspots: 839 groups, 27 ranking-eligible, median Mc 4.50, median b 0.97.
* ST-DBSCAN: 45 clusters, 7,542 noise, ARI 1.000000 on 9,882 events.
* Volcano association: 941 eruptions; sparse-baseline warning recorded.
* Benchmark: 82 measurements; Pandas faster; Spark startup 10.787 s.

## 8. Interpretation and limitations

* All results are descriptive or associational. No earthquake or eruption
  prediction is claimed; no causal effect is established.
* Detection correction depends on estimated Mc and magnitude-scale assumptions.
* Large-sample distributed clustering is a 119,765-event bounded sample, not all
  681,902 events; the reason is the 16 GB RAM limit and is recorded in metrics.
* ARI 0.999991 means near-equivalence, not bit-identical labels; border points
  may legitimately join different clusters.
* Sparse volcano baselines produce wide confidence intervals and unstable
  ratios; FDR q-values and counts are shown.
* Spark local mode is one machine, not a multi-node cluster.
