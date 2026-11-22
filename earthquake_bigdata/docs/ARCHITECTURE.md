# Architecture and data flow

## System architecture

```mermaid
flowchart LR
    USGS[USGS FDSN Event API] --> ING[Resumable Python ingestion]
    GVP[Smithsonian GVP WFS] --> ING
    ING --> RAW[Raw CSV / GeoJSON]
    RAW --> HDFSRAW[HDFS raw layer]
    RAW --> LOCALRAW[Local raw mirror]
    HDFSRAW --> ETL[PySpark ETL + quality report]
    LOCALRAW --> ETL
    ETL --> PQ[HDFS Parquet by year]
    ETL --> LPQ[Local Parquet mirror]
    PQ --> STATS[Spark SQL basic statistics]
    PQ --> HOT[Corrected hotspots: Mc + b-value]
    PQ --> CLUS[Distributed ST-DBSCAN / DBSCAN / K-Means]
    PQ --> VOL[Volcano association]
    STATS --> RES[Saved result tables]
    HOT --> RES
    CLUS --> RES
    VOL --> RES
    RES --> BENCH[Pandas vs Spark benchmark]
    RES --> DASH[Streamlit dashboard]
```

**Important distinction:** Spark runs in `local[6]` mode on a single Windows
machine. Work is partitioned and parallel across threads, but this is not a
multi-node cluster. HDFS is a single NameNode + one DataNode with replication 1.

## Storage layout

```
HDFS
/earthquake/raw/global/{year}/{year}-{month}.csv
/earthquake/raw/stress/{region}/{year}/...
/earthquake/processed/{dataset}/year=.../*.parquet
/volcano/raw/{volcanoes|eruptions}/*.geojson
/volcano/processed/{volcanoes|eruptions}/*.parquet
/results/{hotspots,clustering,volcano,...}
```

The local worker also keeps a mirror under `data/processed/` so the dashboard
does not depend on HDFS being up. Every dataset write records the actual backend
in `results/metadata/`.

## Earthquake ETL flow

```mermaid
flowchart TD
    A[Read monthly raw CSVs] --> B[Cast numeric / parse UTC timestamps]
    B --> C[Measure missingness and invalid coordinates]
    C --> D[Filter type = earthquake]
    D --> E[Validate lat/lon and magnitude]
    E --> F[Deduplicate by id, keep latest updated]
    F --> G[Add year/month/day/hour, 5° and 1° grids, scale family, flags]
    G --> H[Write Parquet partitioned by year to HDFS + local mirror]
    H --> I[Preprocessing report JSON]
```

## Distributed ST-DBSCAN flow

```mermaid
flowchart TD
    A[Events with id, lat, lon, time] --> B[Assign primary + 1-ring halo cells]
    B --> C[Spark groupBy cell_id / applyInPandas]
    C --> D[Local exact ST-DBSCAN per cell]
    D --> E[Local cluster keys cell|label]
    E --> F[Union-Find over keys sharing an event id]
    F --> G[Global stable cluster ids]
    G --> H[Compare with single-machine reference: ARI]
    G --> I[Cluster statistics, maps, sensitivity]
```

## Volcano association flow

```mermaid
flowchart TD
    A[Earthquakes + volcanoes + eruptions] --> B[Filter confirmed day/month eruptions 2000-2025]
    B --> C[Spark 5° grid join + exact Haversine <= radius]
    C --> D[Per-eruption baseline / pre / post counts]
    D --> E[Volcano-specific baseline excluding eruption windows]
    E --> F[Rate ratios, CIs, Poisson tests]
    E --> G[Permutation tests]
    F --> H[Benjamini-Hochberg FDR]
    G --> H
    H --> I[Volcano rankings + radius sensitivity]
```

## Benchmark flow

```mermaid
flowchart LR
    P[Processed Parquet] --> F[Write identical 10/25/50/100% Parquet + CSV]
    F --> PD[Pandas operations]
    F --> SP[Spark operations]
    PD --> T[Raw timings]
    SP --> T
    T --> S[Medians / speed-ups / plots]
```

## Reproducibility controls

* Every path and parameter comes from `config/config.yaml`.
* Downloads are resumable and hashed.
* Every ETL and analytics output is saved as JSON/CSV/Parquet.
* Random operations use explicit seeds.
* Tests use synthetic data only for unit correctness; real data are used for
  integration runs.
* Results files record generation time, parameters and backend.
