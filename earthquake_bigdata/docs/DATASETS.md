# Dataset documentation

Retrieved: **2026-11-19** (UTC). The download scripts record the exact retrieval
time, request parameters, row count, count-API value and SHA-256 for each chunk.

## 1. Primary earthquake dataset

| Item | Value |
|---|---|
| Source | U.S. Geological Survey, FDSN Event Web Service |
| Endpoint | `https://earthquake.usgs.gov/fdsnws/event/1/query` |
| Catalogue | `earthquake` event type retained from all returned types |
| Period | 2000-01-01 to 2025-12-31 |
| Magnitude | `minmagnitude=2.5` |
| Coverage | global |
| Request frequency | one request per calendar month |
| Format | CSV |
| Measured total (count API) | **689,501** events |
| Raw storage | `data/raw/earthquakes/global/{year}/{year}-{month}.csv` |
| Raw HDFS | `/earthquake/raw/global/...` after batch upload |
| Processed | `data/processed/earthquakes/global` (local mirror) and `/earthquake/processed/global` (Parquet, partitioned by year) |

### Fields retained

`time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id,
updated, place, type, status, horizontalError, depthError, magError, magNst,
locationSource, magSource`.

The CSV response is stored byte-for-byte. No raw value is edited at ingestion.

### Cleaning rules (ETL)

* Keep `type == "earthquake"`; quarry blasts, explosions and other types are
  removed and counted.
* Validate latitude in [-90, 90] and longitude in [-180, 180]; invalid rows are
  removed and counted.
* Remove rows without magnitude; missing depth is retained with
  `depth_missing=true` because depth cannot be invented.
* Deduplicate by event `id`, keeping the row with the latest `updated`
  timestamp (the latest catalogue revision).
* Convert `time`/`updated` to UTC; add `year, month, day, hour, day_of_year`.
* Add 5° (`grid_id_5`) and 1° (`grid_id_1`) grid identifiers.
* Add `magnitude_family` from `magType`; add explicit quality flags.
* Save Snappy Parquet partitioned by year.

### Missingness (sample run)

The sample ETL report shows the missingness per field (e.g. `gap`, `dmin`,
`horizontalError` are frequently null). These values are retained and reported;
they are not silently imputed.

## 2. Stress-test earthquake dataset

The stress-test data are kept **separate** and are never used for global
scientific findings.

| Region | Bounding box (lat / lon) | Min magnitude | Measured count |
|---|---|---|---|
| Alaska | 50–72 / -180 to -129 | 1.0 | 731,416 |
| California | 32–43 / -125 to -114 | 1.0 | 692,197 |

The boxes are approximate and include some neighbouring events. They exist to
test scalability (more records, denser clusters), not regional science. The
processed identifiers are `stress_alaska` and `stress_california`; the combined
ETL identifier is `stress_all`.

## 3. Smithsonian Global Volcanism Program

| Item | Value |
|---|---|
| Source | Global Volcanism Program, Smithsonian Institution |
| Access | Official OGC WFS `https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows` |
| Layer | `GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes` |
| Layer | `GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions` |
| Volcanoes | **1,214** |
| Eruptions | **11,089** |
| Citation | Global Volcanism Program, 2026. *Volcanoes of the World* (v. 5.4.0; 7 Aug 2026). DOI recorded in config. |

The public `volcano.si.edu` HTML site returns a bot-verification challenge to
scripts; the official WFS is used instead. It is the same GVP catalogue and
requires no key. Pages are retained as raw JSON; the combined GeoJSON is
derived without changing fields.

### Volcano fields retained

Volcano number, name, latitude, longitude, elevation, country, region,
subregion, landform, primary type, tectonic setting, geologic epoch, evidence
category, last eruption year.

### Eruption fields retained

Volcano/eruption numbers, activity type, VEI, start/end year-month-day,
`StartDateYearUncertainty`, date modifiers, start evidence method, coordinates,
`date_precision`, `date_is_approximate`, `is_confirmed_eruption`,
`duration_days`.

Dates are not invented. When month/day are 0 or null, a joinable date is built
using the first day of the known period **only for computation**, while
`date_precision` records the true precision. Year-only and unconfirmed eruptions
are excluded from the primary volcano rate analysis and reported separately.

## 4. Sample dataset (development)

* `data/sample/earthquakes_10k.csv`: 10,000 real records, deterministic
  proportional stratified sample by 13 source months and magnitude bin.
  Raw rows before sampling: 29,498; unique IDs: 10,000; M 2.5–7.9.
* `data/sample/volcanoes_sample.geojson`: 50 real volcanoes with the most
  eruptions during 2000–2025; 2,736 associated real eruptions.
* `data/sample/sample_manifest.json` records the selection method and hashes.

## 5. Reproducibility and provenance

* Download manifests: `data/raw/earthquakes/global/_manifest.json` and
  per-layer volcano manifests.
* Each chunk has `count_api`, `rows`, `sha256`, `downloaded_at`, URL parameters.
* `ingestion.validate_downloads --strict` checks headers, coordinates, times,
  duplicate IDs, sample size, and GVP feature counts against `numberMatched`.
* No dataset is regenerated from synthetic values. Synthetic data appear only
  in unit tests, where they are labelled as synthetic.
