# Pandas vs Spark benchmark — measured results

Generated: 2026-11-19T16:14:08+00:00

## Hardware and execution mode

* CPU: AMD64 Family 25 Model 117 Stepping 2, AuthenticAMD (8 physical / 16 logical cores)
* RAM: 15.3 GiB
* Platform: Windows-11-10.0.26300-SP0
* Python: 3.12.10
* Spark: local mode, see `benchmark_metadata.json`; **not a multi-node cluster**.
* Measured Spark session startup: 4.982 s

## Method

* Identical Parquet and CSV files were written once from the processed dataset.
* Fractions are deterministic random subsets of the same source rows.
* Load and group-by use 3 repeats and the median; heavier b-value/spatial-join
  operations use one measured run (shown in `raw_timings.csv`).
* Spark load timing includes a `count()` action so lazy evaluation is not hidden.
* Pandas is single-process; Spark is `local[6]`. This is not a like-for-like core-count claim.

## Speed-up (Parquet, Spark cached; positive means Spark faster)

```
 fraction    operation  pandas_seconds  spark_seconds  speedup_spark_over_pandas faster_engine
     0.10  bvalue_grid        0.050331       6.143729                   0.008192        Pandas
     0.25  bvalue_grid        0.115063       8.851578                   0.012999        Pandas
     0.50  bvalue_grid        0.200072       6.808674                   0.029385        Pandas
     1.00  bvalue_grid        0.392978      10.780050                   0.036454        Pandas
     0.10 groupby_year        0.009103       0.145256                   0.062667        Pandas
     0.25 groupby_year        0.020901       0.136525                   0.153096        Pandas
     0.50 groupby_year        0.037990       0.138208                   0.274874        Pandas
     1.00 groupby_year        0.061211       0.145639                   0.420294        Pandas
     0.10 spatial_join        0.405670       6.797989                   0.059675        Pandas
     0.25 spatial_join        0.966732       5.617776                   0.172084        Pandas
     0.50 spatial_join        1.914865       6.047563                   0.316634        Pandas
     1.00 spatial_join        3.377078       7.480608                   0.451444        Pandas
```

## CSV vs Parquet (group-by)

```
engine  format  fraction  median_seconds
pandas parquet       1.0        0.061211
 spark     csv       1.0        0.617143
 spark parquet       1.0        0.145639
```

## Partition sensitivity

```
 fraction  partitions  median_seconds
      1.0         8.0        0.995991
      1.0        16.0        0.966349
```

## Caching

```
 fraction  cached  median_seconds
     0.10    True        0.145256
     0.25    True        0.136525
     0.50    True        0.138208
     1.00   False        0.227156
     1.00    True        0.145639
```

## Clustering overhead (bounded sample)

_No measurements available._

## Interpretation rules

* A speed-up below 1 means **Pandas was faster**; this is reported, not suppressed.
* Spark's fixed startup and scheduling overhead dominates small datasets.
* Timings are machine-specific and should be reproduced with `benchmark_report.py`.
* No benchmark value in this file is hard-coded; all values come from `raw_timings.csv`.

Figures: `figures/scaling_groupby.png`, `figures/format_comparison.png`,
`figures/partition_comparison.png`, `figures/caching_comparison.png`.