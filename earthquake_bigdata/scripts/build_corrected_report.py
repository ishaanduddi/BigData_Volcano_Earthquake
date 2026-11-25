"""Build a corrected DOCX/PDF report from the submitted PDF.

The original PDF has no editable source in the repository.  This script extracts
its text, applies the factual corrections identified during review, and rebuilds
a clean corrected report with the original figures, tables and references.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from pypdf import PdfReader
from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak,
)
from reportlab.lib import colors

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
PDF_IN = ROOT / "Team4_BDA_Report.pdf"
OUT_DOCX = ROOT / "Team4_BDA_Report_CORRECTED.docx"
OUT_PDF = ROOT / "Team4_BDA_Report_CORRECTED.pdf"
ASSETS = ROOT / "report_assets"

CORRECTIONS = [
    (
        "hotspot rankings are corrected for Mc and for the b -value before regions are compared",
        "hotspot rankings are corrected for Mc, with b-values reported alongside, before regions are compared",
    ),
    (
        "the retrieval timestamp, the HTTP status, the row count and a SHA -256 hash",
        "the retrieval timestamp, the row count and a SHA-256 hash",
    ),
    (
        "and fails the run on a mismatch, so the dataset can be rebuilt exactly",
        "and the strict validation command fails on a mismatch; the downloader aborts on material mismatches, so the dataset can be rebuilt exactly",
    ),
    (
        "Their parameter-selection recipe is adopted directly in this work.",
        "Their parameter-selection approach motivates the sensitivity analysis in this work, but the recipe was not implemented directly.",
    ),
    (
        "and the parameter values are chosen with the crossover-region heuristic of Piegari et al. [1].",
        "and the parameter values were seeded at 50 km / 7 days / minPts 10 and evaluated with a sensitivity sweep; Piegari et al. motivated the sensitivity analysis, but their crossover-region selection recipe was not implemented.",
    ),
    (
        "The number of eruptions that are individually significant after false - discovery-rate correction grows with radius simply because more events fall inside the larger window.",
        "The number of eruptions that are individually significant after false-discovery-rate correction grows with radius, consistent with greater statistical power from the larger event count.",
    ),
    (
        "Eighty-eight timed measurements were collected on the full catalogue",
        "Eighty-eight timed measurements were collected across 10%, 25%, 50% and 100% fractions of the catalogue",
    ),
    (
        "reading Parquet in Spark is 3.5 times faster than reading the equivalent CSV (0.195 s against 0.675 s for the group -by)",
        "the Parquet group-by is 3.5 times faster than the equivalent CSV group-by (0.195 s against 0.675 s)",
    ),
    (
        "The number of eruptions that are individually significant after false -discovery-rate correction grows with radius simply because more events fall inside the larger window.",
        "The number of eruptions that are individually significant after false-discovery-rate correction grows with radius, consistent with greater statistical power from the larger event count.",
    ),
    (
        "The volcano analysis finds a small but consistent pre -eruption elevation in seismicity (median rate ratio ≈1.18 within 100 km) that returns to baseline afterwards.",
        "The volcano analysis finds a small but consistent pre-eruption elevation in seismicity at 100 km (median rate ratio 1.18). At 100 km the median post-eruption ratio is 1.015; the 50 km and 200 km post ratios are 0.877 and 1.078.",
    ),
    (
        "five invalid records, leaving 681,902 records",
        "five additional out-of-scope records, leaving 681,902 records",
    ),
]


def corrected(text: str) -> str:
    for old, new in CORRECTIONS:
        text = text.replace(old, new)
    return text


def extract_sections():
    reader = PdfReader(PDF_IN)
    pages = [p.extract_text() or "" for p in reader.pages]
    text = "\n".join(pages)
    lines = [l.rstrip() for l in text.splitlines()]
    title_lines, abstract_lines, index_lines = [], [], []
    sections = []
    mode = "title"
    current_title = None
    current_body = []

    def flush():
        nonlocal current_title, current_body
        if current_title:
            body = " ".join(x.strip() for x in current_body)
            body = re.sub(r"\s+", " ", body).strip()
            if body:
                sections.append((current_title, corrected(body)))
        current_title, current_body = None, []

    heading_re = re.compile(r"^(I|II|III|IV|V)\.\s|^[A-G]\.\s|^REFERENCES$")
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if mode == "title":
            if s.startswith("Abstract—"):
                mode = "abstract"
                abstract_lines.append(s)
            else:
                title_lines.append(s)
        elif mode == "abstract":
            if s.startswith("Index Terms—"):
                mode = "index"
                index_lines.append(s)
            else:
                abstract_lines.append(s)
        elif mode == "index":
            if heading_re.match(s):
                mode = "sections"
                current_title = s
            else:
                index_lines.append(s)
        else:
            if heading_re.match(s):
                flush()
                current_title = s
            else:
                current_body.append(line)
    flush()
    return title_lines, corrected(" ".join(abstract_lines)), corrected(" ".join(index_lines)), sections


TITLE = "Large-Scale Spatial and Temporal Analysis of Global Earthquake and Volcanic Activity Using Hadoop and Apache Spark"
AUTHORS = "Ishaan Balakrishna Duddi, MD Thejas, Siva Guru"
AFFIL = "Department of Computer Science and Engineering, Amrita School of Computing, Bengaluru, Amrita Vishwa Vidyapeetham, India"
ADVISOR = "Project Advisor: Ms. Sangita Khare"


def add_docx_table(doc, headers, rows):
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    for i, h in enumerate(headers):
        table.rows[0].cells[i].text = h
    for row in rows:
        cells = table.add_row().cells
        for i, value in enumerate(row):
            cells[i].text = str(value)
    doc.add_paragraph()


def build_docx(title_lines, abstract, index_terms, sections):
    abstract = re.sub(r"^Abstract[—–-]\s*", "", abstract)
    index_terms = re.sub(r"^Index Terms[—–-]\s*", "", index_terms)
    doc = Document()
    section = doc.sections[0]
    section.top_margin = Inches(0.75)
    section.bottom_margin = Inches(0.75)
    section.left_margin = Inches(0.85)
    section.right_margin = Inches(0.85)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(TITLE)
    r.bold = True
    r.font.size = Pt(17)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run("Team 4 · 5th Semester Big Data Analytics Project").italic = True

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run(AUTHORS)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run(AFFIL)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run(ADVISOR)

    doc.add_heading("Abstract", level=1)
    doc.add_paragraph(abstract)
    doc.add_paragraph(index_terms).runs[0].italic = True

    for heading, body in sections:
        if heading == "REFERENCES":
            doc.add_heading("References", level=1)
            # Split references on [n] markers.
            parts = re.split(r"(?=\[\d+\])", body)
            for part in parts:
                part = part.strip()
                if part:
                    doc.add_paragraph(part)
            continue
        level = 1 if re.match(r"^(I|II|III|IV|V)\.", heading) else 2
        doc.add_heading(heading, level=level)
        # Keep long extracted text readable: split into sentence groups.
        sentences = re.split(r"(?<=[.!?])\s+", body)
        chunk = []
        for sentence in sentences:
            chunk.append(sentence)
            if len(chunk) >= 4:
                doc.add_paragraph(" ".join(chunk))
                chunk = []
        if chunk:
            doc.add_paragraph(" ".join(chunk))

        if heading.startswith("A. Data Sources"):
            for img, caption in [
                (ASSETS / "page2_img0_Image27.png",
                 "Fig. 1. End-to-end architecture. Raw responses are stored unchanged in HDFS, cleaned with PySpark into year-partitioned Parquet, and consumed by four analytics modules whose outputs feed the benchmark and the dashboard. Spark runs in local[6] mode on a single machine."),
            ]:
                if img.exists():
                    doc.add_picture(str(img), width=Inches(6.2))
                    doc.add_paragraph(caption).runs[0].italic = True
        if heading.startswith("B. Detection Correction"):
            for img, caption in [
                (ASSETS / "page4_img0_Image37.png",
                 "Fig. 2. Change in raw event count against change in detection-corrected rate for 198 comparable 5° cells. Points below the horizontal axis are cells where recorded growth was not accompanied by growth in the rate above Mc."),
            ]:
                if img.exists():
                    doc.add_picture(str(img), width=Inches(6.0))
                    doc.add_paragraph(caption).runs[0].italic = True
        if heading.startswith("E. Performance"):
            for img, caption in [
                (ASSETS / "page5_img0_Image40.png",
                 "Fig. 3. Left: ratio of Spark to Pandas runtime for three operations at 681,902 rows. Right: effect of Spark partition count on the group-by operation."),
            ]:
                if img.exists():
                    doc.add_picture(str(img), width=Inches(6.0))
                    doc.add_paragraph(caption).runs[0].italic = True

    # Tables are inserted in the Results section as a compact appendix block,
    # avoiding extraction of the original two-column table text.
    doc.add_heading("Result tables", level=1)
    add_docx_table(doc, ["Property", "Value"], [
        ("Raw records downloaded", "689,501"),
        ("Records after cleaning", "681,902"),
        ("Non-earthquake events removed", "7,594"),
        ("Mean / median magnitude", "3.81 / 4.0"),
        ("Maximum magnitude", "9.1"),
        ("Mean / median depth", "56.7 km / 20.35 km"),
        ("Occupied 5° cells", "1,407"),
        ("Magnitude types / networks", "22 / 22"),
    ])
    add_docx_table(doc, ["Outcome (198 comparable cells)", "Cells"], [
        ("Growth persists after correction", "96"),
        ("Decline persists", "53"),
        ("Growth disappears after correction", "44"),
        ("Decline reverses after correction", "5"),
    ])
    add_docx_table(doc, ["Method", "Clusters", "Noise", "Time (s)", "ARI"], [
        ("ST-DBSCAN (distributed)", "494", "98,583", "154.04", "0.999991"),
        ("ST-DBSCAN (reference)", "504", "98,583", "24.65", "1.000000"),
        ("DBSCAN (spatial only)", "204", "6,367", "12.79", "0.081"),
        ("K-Means (k=25)", "25", "0", "1.00", "0.018"),
    ])
    add_docx_table(doc, ["Radius", "n", "Median pre RR", "Median post RR", "Signif. pre / post"], [
        ("50 km", "413", "1.15", "0.88", "47 / 45"),
        ("100 km", "569", "1.18", "1.02", "127 / 126"),
        ("200 km", "626", "1.12", "1.08", "235 / 258"),
    ])
    add_docx_table(doc, ["Operation", "Pandas (s)", "Spark (s)", "Spark slower by"], [
        ("Group-by year", "0.034", "0.195", "5.8×"),
        ("100 km spatial join", "1.144", "8.042", "7.0×"),
        ("Per-grid b-value", "0.325", "10.433", "32.1×"),
    ])

    p = doc.add_paragraph()
    p.add_run("Corrected version: factual and methodological corrections applied after review; figures are reproduced from the submitted report.").italic = True
    doc.save(OUT_DOCX)
    print("Saved", OUT_DOCX)


def build_pdf(title_lines, abstract, index_terms, sections):
    abstract = re.sub(r"^Abstract[—–-]\s*", "", abstract)
    index_terms = re.sub(r"^Index Terms[—–-]\s*", "", index_terms)
    font_dir = Path(r"C:\Windows\Fonts")
    pdfmetrics.registerFont(TTFont("Arial", str(font_dir / "arial.ttf")))
    pdfmetrics.registerFont(TTFont("Arial-Bold", str(font_dir / "arialbd.ttf")))
    pdfmetrics.registerFont(TTFont("Arial-Italic", str(font_dir / "ariali.ttf")))
    pdfmetrics.registerFont(TTFont("Arial-BoldItalic", str(font_dir / "arialbi.ttf")))
    pdfmetrics.registerFontFamily("Arial", normal="Arial", bold="Arial-Bold", italic="Arial-Italic", boldItalic="Arial-BoldItalic")
    styles = getSampleStyleSheet()
    for name, font in (("Normal", "Arial"), ("BodyText", "Arial"), ("Title", "Arial-Bold"),
                       ("Heading1", "Arial-Bold"), ("Heading2", "Arial-Bold"), ("Italic", "Arial-Italic")):
        styles[name].fontName = font
    styles.add(ParagraphStyle(name="SmallItalic", parent=styles["BodyText"], fontName="Arial-Italic",
                              fontSize=8, leading=10, textColor=colors.grey))

    doc = SimpleDocTemplate(str(OUT_PDF), pagesize=A4,
                            leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                            topMargin=0.65 * inch, bottomMargin=0.65 * inch,
                            title="Corrected Big Data Project Report")
    story = []
    story.append(Paragraph(TITLE, styles["Title"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph("Team 4 · 5th Semester Big Data Analytics Project", styles["Italic"]))
    story.append(Paragraph(AUTHORS, styles["Normal"]))
    story.append(Paragraph(AFFIL, styles["Normal"]))
    story.append(Paragraph(ADVISOR, styles["Normal"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Abstract", styles["Heading1"]))
    story.append(Paragraph(abstract.replace("&", "&amp;"), styles["BodyText"]))
    story.append(Paragraph(index_terms.replace("&", "&amp;"), styles["Italic"]))
    story.append(Spacer(1, 8))

    def image(path, caption):
        if path.exists():
            story.append(Image(str(path), width=6.3 * inch, height=3.1 * inch, kind="proportional"))
            story.append(Paragraph(caption, styles["SmallItalic"]))
            story.append(Spacer(1, 8))

    for heading, body in sections:
        if heading == "REFERENCES":
            story.append(Paragraph("References", styles["Heading1"]))
            parts = re.split(r"(?=\[\d+\])", body)
            for part in parts:
                if part.strip():
                    story.append(Paragraph(part.strip().replace("&", "&amp;"), styles["BodyText"]))
            continue
        style = styles["Heading1"] if re.match(r"^(I|II|III|IV|V)\.", heading) else styles["Heading2"]
        story.append(Paragraph(heading, style))
        for sentence_block in re.split(r"(?<=[.!?])\s+", body):
            pass
        # Paragraph chunks to avoid one giant flowable.
        words = body.split()
        chunks = [" ".join(words[i:i + 95]) for i in range(0, len(words), 95)]
        for chunk in chunks:
            story.append(Paragraph(chunk.replace("&", "&amp;"), styles["BodyText"]))
        if heading.startswith("A. Data Sources"):
            image(ASSETS / "page2_img0_Image27.png", "Fig. 1. End-to-end architecture (reproduced from the submitted report).")
        if heading.startswith("B. Detection Correction"):
            image(ASSETS / "page4_img0_Image37.png", "Fig. 2. Raw vs detection-corrected change for 198 comparable cells (reproduced from the submitted report).")
        if heading.startswith("E. Performance"):
            image(ASSETS / "page5_img0_Image40.png", "Fig. 3. Runtime ratio and partition sensitivity (reproduced from the submitted report).")

    # Tables
    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("FONTNAME", (0, 0), (-1, -1), "Arial"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ])
    story.append(Paragraph("Result tables", styles["Heading1"]))
    for headers, rows in [
        (["Property", "Value"], [("Raw records", "689,501"), ("After cleaning", "681,902"),
                                  ("Non-earthquake removed", "7,594"), ("Mean/median magnitude", "3.81 / 4.0"),
                                  ("Max magnitude", "9.1"), ("Mean/median depth", "56.7 km / 20.35 km"),
                                  ("Occupied 5° cells", "1,407"), ("Magnitude types / networks", "22 / 22")]),
        (["Outcome (198 cells)", "Cells"], [("Growth persists", "96"), ("Decline persists", "53"),
                                             ("Growth disappears", "44"), ("Decline reverses", "5")]),
        (["Method", "Clusters", "Noise", "Time (s)", "ARI"], [("ST-DBSCAN distributed", "494", "98,583", "154.04", "0.999991"),
                                                               ("ST-DBSCAN reference", "504", "98,583", "24.65", "1.000000"),
                                                               ("DBSCAN spatial", "204", "6,367", "12.79", "0.081"),
                                                               ("K-Means k=25", "25", "0", "1.00", "0.018")]),
        (["Radius", "n", "Median pre RR", "Median post RR", "Significant pre/post"], [("50 km", "413", "1.15", "0.88", "47 / 45"),
                                                                                       ("100 km", "569", "1.18", "1.02", "127 / 126"),
                                                                                       ("200 km", "626", "1.12", "1.08", "235 / 258")]),
        (["Operation", "Pandas (s)", "Spark (s)", "Spark slower by"], [("Group-by year", "0.034", "0.195", "5.8x"),
                                                                         ("100 km spatial join", "1.144", "8.042", "7.0x"),
                                                                         ("Per-grid b-value", "0.325", "10.433", "32.1x")]),
    ]:
        data = [headers] + [[str(c) for c in row] for row in rows]
        t = Table(data, repeatRows=1)
        t.setStyle(table_style)
        story.append(t)
        story.append(Spacer(1, 8))

    story.append(Paragraph(
        "Corrected version: factual and methodological corrections applied after review; figures are reproduced from the submitted report.",
        styles["SmallItalic"],
    ))
    doc.build(story)
    print("Saved", OUT_PDF)


def main():
    title_lines, abstract, index_terms, sections = extract_sections()
    build_docx(title_lines, abstract, index_terms, sections)
    build_pdf(title_lines, abstract, index_terms, sections)


if __name__ == "__main__":
    main()
