"""Apply factual corrections to Team4_Final_Presentation.pptx.

Run from the project root:
    .venv\\Scripts\\python.exe scripts\\patch_presentation.py
A timestamped backup of the original PPTX is created first.
"""
from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path

from pptx import Presentation

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PPTX = ROOT / "Team4_Final_Presentation.pptx"


def replace_in_paragraph(paragraph, old: str, new: str) -> bool:
    if old not in paragraph.text:
        return False
    # Prefer a single-run replacement so bold/italic/font settings survive.
    for run in paragraph.runs:
        if old in run.text:
            run.text = run.text.replace(old, new)
            return True
    # Cross-run fallback: rebuild the paragraph text on the first run.
    full = paragraph.text.replace(old, new)
    if paragraph.runs:
        paragraph.runs[0].text = full
        for run in paragraph.runs[1:]:
            run.text = ""
    else:
        paragraph.text = full
    return True


def replace_everywhere(prs, old: str, new: str) -> int:
    count = 0
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    if replace_in_paragraph(paragraph, old, new):
                        count += 1
            if getattr(shape, "has_table", False):
                for row in shape.table.rows:
                    for cell in row.cells:
                        for paragraph in cell.text_frame.paragraphs:
                            if replace_in_paragraph(paragraph, old, new):
                                count += 1
    return count


def main() -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = PPTX.with_name(f"Team4_Final_Presentation_pre_correction_{stamp}.pptx")
    shutil.copy2(PPTX, backup)
    prs = Presentation(PPTX)

    replacements = [
        # Slide 5 / Piegari: we did not implement the crossover-region recipe.
        (
            "Relevance: This is our Module 7 blueprint. It replaces guesswork in parameter selection with the crossover-region rule, warns that unscaled depth and latitude distort distances (the same failure as treating lat/lon as flat XY), and shows that 3-D structure disappears in a 2-D map — so we cluster on rescaled coordinates and report both.",
            "Relevance: This is our Module 7 reference. It motivates a sensitivity analysis of parameters, warns that unscaled depth and latitude distort distances, and shows that 3-D structure disappears in a 2-D map. We did not implement its full crossover-region parameter-selection recipe; we seeded 50 km / 7 days / minPts 10 and evaluated sensitivity.",
        ),
        # Slide 6 / Nicolis: F1 protocol is inspired, not the exact protocol.
        (
            "We adopt its F1-based evaluation protocol, and adaptive radius is our planned stretch goal.",
            "We use an F1-style comparison against a simplified Gardner–Knopoff-style reference; this is not the paper’s exact ST-DBSCAN-EV protocol. Adaptive radius remains a stretch goal.",
        ),
        # Slide 8 / Hu: partitioning is by year only.
        (
            "It tells us to write Parquet partitioned by year/month (and optionally a coarse 5° geospatial cell) so that Spark performs partition pruning, and it gives us the vocabulary — data locality, block pruning, spatial partitioning — that our Module 10 experiments measure (CSV vs partitioned Parquet).",
            "It motivates partitioning for pruning. Our Parquet is partitioned by year only, not by month or 5° cells; the paper provides the vocabulary used in the CSV-vs-Parquet benchmark.",
        ),
        # Slide 10 / Garani: no formal dimension tables.
        (
            "We keep one fact table of events plus small dimension tables (region, time bucket, volcano) and — following this paper — choose the schema by measuring query time rather than assuming, which also gives us a second, defensible axis for the Module 10 evaluation.",
            "We use year-partitioned Parquet event tables plus separate volcano and eruption tables; we did not build a formal star schema or dimension tables. The paper motivates measuring schema choices rather than assuming them.",
        ),
        # Slide 11 / a-positive: not implemented.
        (
            "this gives us both the method and the citation to normalise rate, trend and hotspot intensity before ranking regions — and it is our defence against the false claim that “earthquakes are increasing”.",
            "it motivates detection correction and is our defence against the false claim that “earthquakes are increasing”. We implement maximum-curvature Mc; we do not implement the a-positive estimator.",
        ),
        # Slide 12 / Omori-Bath: optional module not implemented.
        (
            "The methodological basis for our optional Module 8. Where the proposal’s rule (“a large event, then nearby events within X days”) is a heuristic, subsequence clustering groups candidate sequences by their decay signature, and its output feeds our aftershock timeline and Omori-decay fit directly.",
            "Future-work motivation. The optional Omori/Båth aftershock module was not implemented; no aftershock timeline or Omori fit is reported in this project.",
        ),
        # Slide 14: soften unsupported novelty claim.
        (
            "Nothing connects HDFS ingestion → Spark ETL → distributed density clustering on a global multi-decade catalog.",
            "We did not find a directly comparable end-to-end implementation in the reviewed literature.",
        ),
        # Slide 20: benchmarks are local, not on HDFS.
        (
            "Results:  /results/{hotspots, clustering, volcano, benchmarks}",
            "Results:  /results/{hotspots, clustering, volcano}; benchmarks are local",
        ),
        # Slide 22: dominant family, not strictly single scale.
        (
            "b-value from Aki–Utsu; only single-scale cells are ranked",
            "b-value from Aki–Utsu; cells with a dominant scale family (≥75%) are ranked",
        ),
        # Slide 23: do not call corrected changes “real”.
        (
            "raw rose and corrected rate rose — a real increase",
            "raw rose and corrected rate rose — growth persists after correction",
        ),
        (
            "both fell — a real decrease",
            "both fell — decline persists after correction",
        ),
        # Slide 31: wrong cluster attributed to M 9.1.
        (
            "Largest cluster: 2,257 events (one holds M 9.1)",
            "Largest cluster: 2,257 events, max M 4.75 (Hawaii 2018); M 9.1 is in a 476-event cluster",
        ),
        # Slide 32: radius-specific post ratio and significance wording.
        (
            "•  After the eruption it returns to ≈ 1.0; 127 of 569 are individually significant at 100 km.",
            "•  At 100 km median post ratio is 1.015; 127 of 569 pre-eruption tests are significant. Post ratios are 0.877 at 50 km and 1.078 at 200 km.",
        ),
        # Slide 33: benchmark scope and Parquet wording.
        (
            "88 runs  at 681,902 rows",
            "88 measurements across 10/25/50/100% of 681,902 rows",
        ),
        (
            "Parquet reads 3.5× faster than CSV",
            "Parquet group-by is 3.5× faster than CSV",
        ),
        # Slide 37: association, not a “real” causal link.
        (
            "Volcano link is real but small (rate ratio ≈ 1.18)",
            "Volcano association is small but consistent (median pre rate ratio ≈ 1.18 at 100 km)",
        ),
    ]

    total = 0
    for old, new in replacements:
        n = replace_everywhere(prs, old, new)
        total += n
        if n == 0:
            print(f"WARNING: replacement not found: {old[:90]}...")
        else:
            print(f"replaced {n} occurrence(s): {old[:70]}...")

    prs.save(PPTX)
    print(f"Saved corrected PPTX: {PPTX}")
    print(f"Backup: {backup}")
    print(f"Total paragraph/cell replacements: {total}")


if __name__ == "__main__":
    main()
