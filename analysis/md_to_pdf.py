#!/usr/bin/env python3
"""Convert an EDA markdown report to PDF with embedded images and clean styling.

Usage:
    python md_to_pdf.py                         # defaults: eda_report.md → eda_report.pdf
    python md_to_pdf.py input.md output.pdf
"""
import sys, os, re
from pathlib import Path

import markdown
from weasyprint import HTML

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_IN  = SCRIPT_DIR / "eda_report.md"
DEFAULT_OUT = SCRIPT_DIR / "eda_report.pdf"

md_path  = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IN
pdf_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT

if not md_path.exists():
    print(f"ERROR: {md_path} not found"); sys.exit(1)

md_text = md_path.read_text(encoding="utf-8")

# ── Strip GitHub alert syntax (> [!NOTE] etc.) → styled divs ─────────────────
ALERT_RE = re.compile(
    r"^> \[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*\n"
    r"((?:^>.*\n?)+)",
    re.MULTILINE,
)

ALERT_COLOURS = {
    "NOTE":      ("#e8f4fd", "#1f6feb", "ℹ️"),
    "TIP":       ("#dafbe1", "#1a7f37", "💡"),
    "IMPORTANT": ("#fff8c5", "#9a6700", "❗"),
    "WARNING":   ("#fff1e5", "#bc4c00", "⚠️"),
    "CAUTION":   ("#ffebe9", "#cf222e", "🛑"),
}

def _alert_to_html(m: re.Match) -> str:
    kind = m.group(1)
    body_lines = m.group(2).strip().split("\n")
    body = "\n".join(l.lstrip("> ").rstrip() for l in body_lines)
    bg, border, icon = ALERT_COLOURS.get(kind, ("#f6f8fa", "#666", ""))
    return (
        f'<div class="alert" style="background:{bg}; border-left:4px solid {border}; '
        f'padding:12px 16px; margin:16px 0; border-radius:6px;">'
        f'<strong>{icon} {kind}</strong><br/>{body}</div>\n'
    )

md_text = ALERT_RE.sub(_alert_to_html, md_text)

# ── Convert image paths to absolute file:// URIs ─────────────────────────────
IMG_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")

def _fix_img(m: re.Match) -> str:
    alt, src = m.group(1), m.group(2)
    if not src.startswith(("http://", "https://", "file://")):
        abs_path = (md_path.parent / src).resolve()
        src = abs_path.as_uri()
    return f"![{alt}]({src})"

md_text = IMG_RE.sub(_fix_img, md_text)

# ── Render Markdown → HTML ────────────────────────────────────────────────────
html_body = markdown.markdown(
    md_text,
    extensions=["tables", "fenced_code", "toc", "sane_lists"],
)

# ── Wrap in full HTML document with print-friendly CSS ────────────────────────
CSS = """
@page {
    size: A4;
    margin: 20mm 18mm 20mm 18mm;
    @bottom-center { content: counter(page) " / " counter(pages); font-size: 9pt; color: #666; }
}
body {
    font-family: "Inter", "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    font-size: 10.5pt;
    line-height: 1.55;
    color: #1f2328;
    max-width: 100%;
}
h1 { font-size: 20pt; border-bottom: 2px solid #d0d7de; padding-bottom: 6px; page-break-after: avoid; }
h2 { font-size: 16pt; border-bottom: 1px solid #d0d7de; padding-bottom: 4px; margin-top: 28px;
     page-break-after: avoid; }
h3 { font-size: 13pt; margin-top: 20px; page-break-after: avoid; }
h4 { font-size: 11pt; margin-top: 16px; page-break-after: avoid; }

table {
    border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 9.5pt;
    page-break-inside: auto;
}
th, td { border: 1px solid #d0d7de; padding: 5px 8px; text-align: left; }
th { background: #f6f8fa; font-weight: 600; }
tr { page-break-inside: avoid; }

code { background: #f6f8fa; padding: 1px 5px; border-radius: 4px; font-size: 9pt;
       font-family: "JetBrains Mono", "Fira Code", "Consolas", monospace; }
pre { background: #f6f8fa; padding: 12px; border-radius: 6px; overflow-x: auto;
      font-size: 8.5pt; line-height: 1.45; }
pre code { background: none; padding: 0; }

img { max-width: 100%; height: auto; display: block; margin: 10px auto;
      page-break-inside: avoid; }

hr { border: none; border-top: 1px solid #d0d7de; margin: 24px 0; }

blockquote { border-left: 3px solid #d0d7de; margin: 12px 0; padding: 4px 16px;
             color: #57606a; }

.alert { page-break-inside: avoid; font-size: 9.5pt; }

/* Tighten lists */
ul, ol { padding-left: 20px; }
li { margin-bottom: 3px; }

a { color: #0969da; text-decoration: none; }
strong { color: #1f2328; }
"""

full_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"/><style>{CSS}</style></head>
<body>{html_body}</body>
</html>"""

# ── Render PDF ────────────────────────────────────────────────────────────────
print(f"Converting {md_path.name} → {pdf_path.name} ...")
HTML(string=full_html, base_url=str(md_path.parent)).write_pdf(str(pdf_path))
size_mb = pdf_path.stat().st_size / 1048576
print(f"✅ PDF written: {pdf_path}  ({size_mb:.1f} MB)")
