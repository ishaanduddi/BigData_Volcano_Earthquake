# Pandas vs Spark benchmark — measured results

Generated: 2026-11-19T16:53:36+00:00

## Hardware and execution mode

* CPU: AMD64 Family 25 Model 117 Stepping 2, AuthenticAMD (8 physical / 16 logical cores)
* RAM: 15.3 GiB
* Platform: Windows-11-10.0.26300-SP0
* Python: 3.12.10
* Spark: local mode, see `benchmark_metadata.json`; **not a multi-node cluster**.
* Measured Spark session startup: 5.173 s

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
     0.10  bvalue_grid        0.086838       7.132940                   0.012174        Pandas
     0.25  bvalue_grid        0.169344       6.318679                   0.026800        Pandas
     0.50  bvalue_grid        0.256541       6.656631                   0.038539        Pandas
     1.00  bvalue_grid        0.324873      10.433283                   0.031138        Pandas
     0.10 groupby_year        0.005278       0.164537                   0.032080        Pandas
     0.25 groupby_year        0.008334       0.106546                   0.078218        Pandas
     0.50 groupby_year        0.015795       0.110589                   0.142825        Pandas
     1.00 groupby_year        0.033614       0.195125                   0.172269        Pandas
     0.10 spatial_join        0.121379       4.742612                   0.025593        Pandas
     0.25 spatial_join        0.261578       4.146300                   0.063087        Pandas
     0.50 spatial_join        0.519093       4.866994                   0.106656        Pandas
     1.00 spatial_join        1.144380       8.041723                   0.142305        Pandas
```

## CSV vs Parquet (group-by)

```
engine  format  fraction  median_seconds
pandas     csv       1.0        0.027992
pandas parquet       1.0        0.033614
 spark     csv       1.0        0.675266
 spark parquet       1.0        0.195125
```

## Partition sensitivity

```
 fraction  partitions  median_seconds
      1.0         8.0        1.315013
      1.0        16.0        1.281484
      1.0        32.0        1.961620
      1.0        64.0        3.413230
```

## Caching

```
 fraction  cached  median_seconds
     0.10    True        0.164537
     0.25    True        0.106546
     0.50    True        0.110589
     1.00   False        0.245457
     1.00    True        0.195125
```

## Clustering overhead (bounded sample)

```
 rows engine   seconds result                                       notes
50000 pandas  1.917415    175                    single-machine reference
50000  spark 35.729187    167 distributed local clustering + driver merge
```

## Interpretation rules

* A speed-up below 1 means **Pandas was faster**; this is reported, not suppressed.
* Spark's fixed startup and scheduling overhead dominates small datasets.
* Timings are machine-specific and should be reproduced with `benchmark_report.py`.
* No benchmark value in this file is hard-coded; all values come from `raw_timings.csv`.

Figures: `figures/scaling_groupby.png`, `figures/format_comparison.png`,
`figures/partition_comparison.png`, `figures/caching_comparison.png`.