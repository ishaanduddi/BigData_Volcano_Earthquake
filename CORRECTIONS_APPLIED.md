# Corrections applied to the paper and presentation

Date: 2026-11-25

## Files updated

| File | What changed |
|---|---|
| `earthquake_bigdata/Team4_Final_Presentation.pptx` | Factual corrections applied in place |
| `earthquake_bigdata/Team4_Final_Presentation.pdf` | Re-exported from the corrected PPTX |
| `earthquake_bigdata/Team4_BDA_Report.pdf` | Replaced with the corrected report |
| `earthquake_bigdata/Team4_BDA_Report_CORRECTED.docx` | Editable corrected report |
| `earthquake_bigdata/Team4_BDA_Report_CORRECTED.pdf` | Corrected report PDF |

Originals are retained as `*_pre_correction.*` files.

## Corrected factual points

1. **Largest cluster**
   - Old: “2,257 events (one holds M 9.1)”
   - Correct: **2,257 events, max M 4.75 (Hawaii 2018)**. The M 9.1 Tohoku event is in a **476-event** cluster.

2. **Parquet partitioning**
   - Old: year/month
   - Correct: **year only**.

3. **Dimension tables**
   - Old: “one fact table plus dimension tables”
   - Correct: **year-partitioned Parquet event tables plus separate volcano/eruption tables; no formal star schema or dimension tables.**

4. **a-positive estimator**
   - Old: implied implementation
   - Correct: **not implemented**. The project uses maximum-curvature Mc and Aki–Utsu b-values; a-positive is cited as motivation only.

5. **Omori/Båth optional module**
   - Old: implied it feeds an aftershock timeline
   - Correct: **not implemented**.

6. **b-value correction**
   - Old: hotspots “corrected for Mc and b-value”
   - Correct: **Mc-corrected**; b-values are reported alongside.

7. **Piegari parameter recipe**
   - Old: “adopted directly”
   - Correct: parameters were **seeded at 50 km / 7 days / minPts 10 and evaluated with a sensitivity sweep**; the full crossover-region recipe was not implemented.

8. **Benchmark scope**
   - Old: “88 runs at 681,902 rows”
   - Correct: **88 measurements across 10/25/50/100% fractions** of the 681,902-row catalogue.

9. **Parquet vs CSV**
   - Old: “Parquet reads 3.5× faster”
   - Correct: the **group-by** is 3.5× faster (0.195 s vs 0.675 s), not a pure read benchmark.

10. **Volcano significance wording**
    - Correct: the growth in significant eruptions with radius is **consistent with greater statistical power**, not asserted as the sole cause.

11. **Volcano post-eruption ratio**
    - At 100 km the median post ratio is **1.015**; at 50 km **0.877**; at 200 km **1.078**.

12. **Novelty wording**
    - “Nothing connects …” softened to: **“We did not find a directly comparable end-to-end implementation in the reviewed literature.”**

## Other wording changes

- “Real increase/decrease” → **“persists after correction”**, because the corrected rate is not proof of a physical change.
- “Volcano link is real” → **“association is small but consistent”**.
- HDFS results path no longer claims benchmarks are on HDFS; benchmarks are stored locally.
- “Only single-scale cells are ranked” → **“cells with a dominant scale family (≥75%) are ranked”**.
- Manifest wording no longer claims an HTTP status is stored.
- Validation wording now distinguishes strict validation from the downloader’s material-mismatch abort.

## Note on the corrected report PDF

The original PDF had no editable source in the repository. The corrected report
was rebuilt from the original text with the corrections above and the original
figures. The original PDF is preserved as
`Team4_BDA_Report_pre_correction.pdf`.
