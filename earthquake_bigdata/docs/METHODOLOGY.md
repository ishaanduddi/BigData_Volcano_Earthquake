# Experimental methodology

This document defines the algorithms, parameters and evaluation rules used by
the code. All scientific results are computed from the real downloaded records;
synthetic values are used only in unit tests and are labelled there.

## 1. Magnitude of completeness (Mc)

For each 5° cell and time window, magnitudes are binned at 0.1 units. The
maximum-curvature method selects the mode of the non-cumulative
frequency–magnitude distribution:

```
Mc = M_mode + 0.2
```

The +0.2 correction follows Woessner & Wiemer (2005). A cell/window needs at
least 50 events to be eligible. Bootstrap resampling (200 replicates) gives 95%
percentile intervals for Mc.

**Validation.** A binned Gutenberg–Richter fit above Mc reports:

* `a`, `b_gr`, R² from `log10(N) = a - bM` using cumulative counts;
* a Kolmogorov–Smirnov distance between the observed CDF and the GR exponential
  CDF, with an asymptotic p-value.

These diagnostics are reported; they are not proof that Mc is exact.

## 2. b-value

The Aki–Utsu maximum-likelihood estimator with Utsu's binning correction is:

```
b = log10(e) / (mean(M) - (Mc - ΔM/2)),   M >= Mc, ΔM = 0.1
```

Uncertainty uses the Shi & Bolt (1982) approximation:

```
σ_b = 2.3 b² sqrt( Σ(M_i - mean(M))² / (n(n-1)) )
```

Bootstrap percentile intervals are also reported. A cell needs at least 50
events above Mc for a b-value.

## 3. Magnitude-scale policy

Magnitude scales are not combined blindly. `magType` is mapped to families
(moment, body, surface, local, duration, other). A cell/window is ranked only if
one family accounts for at least 75% of events (`dominant_family` policy).
Other cells are retained with `scale_policy_pass=false` and excluded from
rankings. This is a conservative operational rule, not a physical conversion.

## 4. Corrected hotspot rate

For cell area `A` (spherical lat/lon rectangle, km²) and window duration `T`
(years):

```
raw_rate       = N_all / (A*T) * 1000
corrected_rate = N(M >= Mc) / (A*T) * 1000
```

Rates are per 1000 km² per year. Rankings use raw rate and corrected rate
separately. Growth between the first and second windows is classified as:

* `apparent_growth_persists` — both raw and corrected rates rise;
* `apparent_growth_disappears_after_correction` — raw rises, corrected does not;
* `apparent_decline_reverses_after_correction` — raw falls, corrected rises;
* `decline_persists` — both do not rise.

Corrected rates are an approximation; they do not measure moment release or
remove all detection effects.

## 5. ST-DBSCAN

### Neighbourhood

For event p, q is a neighbour iff:

```
haversine(p, q) <= eps_spatial_km
AND |t_p - t_q| <= eps_temporal_days
```

`minPts` includes p itself. Core points have at least `minPts` neighbours;
border points are density-reachable from a core point but are not core; all
others are noise. Haversine distance is computed on a sphere with
R = 6371.0088 km; lat/lon are never treated as Cartesian coordinates.

Default parameters: **50 km, 7 days, minPts 10**. These are starting values, not
universal truths; sensitivity over 25/50/100 km, 3/7/14 days and minPts 5/10/20
is saved in `results/clustering/parameter_sensitivity.csv`.

### Distributed algorithm

1. Assign each event to a primary spatial cell and to overlapping halo cells.
   Cells are latitude bands with longitude width adjusted to ~75 km in physical
   distance. With cell width ≥ eps, a 1-ring halo contains every possible
   spatial neighbour of a point in the cell.
2. `groupBy(cell_id).applyInPandas(...)` runs the exact local ST-DBSCAN in
   Spark partitions. This is genuine partitioned computation, not a loop on the
   driver.
3. Each local cluster gets a key `(cell_id, local_label)`.
4. A Union–Find structure on the driver merges keys that share an event id.
5. Stable global cluster ids are assigned to the merged components. The merge
   is driver-side and this is reported in the metrics.

### Validation

* Synthetic tests cover two clusters, one chain, noise, time separation, space
  separation, border/core points, cell halos and a cluster crossing a partition
  boundary.
* The distributed partition is compared with the single-machine reference using
  **Adjusted Rand Index (ARI)**. ARI = 1.0 means identical partitions up to
  arbitrary label renaming.
* Cluster labels are arbitrary; they are not semantic identities.

### Comparison algorithms

* **DBSCAN**: identical Haversine neighbour test, temporal constraint removed.
* **K-Means**: 3-D unit vectors, k = 25, ignores time. It is a baseline, not a
  space–time method.
* **Reference declustering**: events processed largest-to-smallest; a seed
  claims unassigned events within Gardner–Knopoff-style windows
  `L(M)=10^(0.1238M+0.983)` km and `T(M)=10^(0.032M+2.738)` days. Clusters with
  fewer than two events are noise. This is a simplified reference, not the full
  published GK algorithm and not ground truth. Cluster-level F1 after optimal
  matching is reported.

## 6. Volcano–earthquake association

For each confirmed eruption with day/month precision inside 2000–2025:

1. Match earthquakes within 50/100/200 km using an exact Haversine test after a
   Spark 5° grid join.
2. **Baseline:** five years before the eruption, clipped to the earthquake
   observation period, excluding all eruption windows (pre 90 days + post 180
   days) of the same volcano.
3. **Pre:** 90 days before the eruption start; **post:** 180 days after.
4. Rates = events / actually observed days. Days are clipped at the observation
   boundaries; they are not forced to 90/180.
5. Rate ratio = window rate / baseline rate.
6. Confidence interval: log-normal Poisson approximation
   `SE = sqrt(1/a + 1/b)` with 0.5 continuity correction when a count is zero.
7. Poisson rate-ratio test: two-sided conditional binomial test.
8. Permutation test: shift the 90-day window randomly within the observed
   baseline, excluding eruption windows; 499 permutations; plus-one p-value.
9. Benjamini–Hochberg FDR q-values are computed across eruptions for each
   radius.
10. Volcano rankings use the median pre-eruption rate ratio among eruptions with
    a sufficient baseline. They are ranking aids, not proof of anomalies.

Overlapping eruption windows, short observation periods, date uncertainty and
sparse counts are reported. This is observational association, **not eruption
prediction and not causation**.

## 7. Benchmark methodology

* Identical deterministic Parquet and CSV files are written once; both engines
  read the same rows.
* Fractions: 10%, 25%, 50%, 100%.
* Spark partitions: 8, 16, 32, 64 (full run).
* Formats: CSV and Parquet.
* Caching: Spark cached vs uncached.
* Operations: load, group-by, per-grid b-value, spatial join, ST-DBSCAN.
* Repeats: 3 for load/group-by; one run for heavier operations; medians used.
* Spark load timing includes a `count()` action so lazy evaluation is not
  mistaken for loading.
* Hardware and Spark startup are recorded. Spark local mode is one machine, not
  a multi-node cluster.
* Speed-up = Pandas time / Spark time. Values below 1 (Pandas faster) are
  reported, not suppressed.

## 8. Statistical and scientific limitations

* Mc depends on the chosen method, bin width and sample size.
* Reported magnitudes come from different scales; the dominant-family rule
  reduces but does not eliminate this issue.
* Earthquake catalogue detection varies in space and time.
* Volcano baseline counts are often sparse; confidence intervals are wide.
* Multiple comparisons are addressed with FDR, but this does not remove
  confounding.
* All results are descriptive/associational. No prediction is claimed.
