#!/usr/bin/env python3
"""Generate a CHARM PDF report and run basic validity checks.

Usage
-----
    python scripts/generate_report.py [--proc-dir demo/processed] [--out-dir data/reports]

Checks
------
- Output starts with ``%PDF``
- Output size is non-trivial (> 10 KB)
- Prints fingerprint, output path, and size
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main():
    parser = argparse.ArgumentParser(description="Generate and validate a CHARM PDF report.")
    parser.add_argument(
        "--proc-dir",
        default=str(PROJECT_ROOT / "demo" / "processed"),
        help="Path to the processed data directory (default: demo/processed)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(PROJECT_ROOT / "data" / "reports"),
        help="Output directory for the PDF (default: data/reports)",
    )
    args = parser.parse_args()

    proc_dir = Path(args.proc_dir)
    out_dir = Path(args.out_dir)

    if not proc_dir.exists():
        print(f"ERROR: processed directory does not exist: {proc_dir}")
        sys.exit(1)

    from reports.context import build_report_context, compute_report_fingerprint
    from reports.pdf_report import render_report_pdf

    # Compute fingerprint
    fingerprint = compute_report_fingerprint(proc_dir)
    print(f"Fingerprint : {fingerprint}")

    # Build context
    ctx = build_report_context(proc_dir)
    print(f"KPIs        : {len(ctx.get('kpis', []))}")
    print(f"Exec bullets: {len(ctx.get('executive_summary', []))}")
    print(f"Top skills  : {len(ctx.get('top_skills', []))}")
    print(f"Emerging    : {len(ctx.get('emerging_skills', []))}")
    print(f"Implications: {len(ctx.get('implications', []))}")

    # Render PDF
    pdf_bytes = render_report_pdf(ctx)
    size = len(pdf_bytes)

    # Write to disk
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"CHARM_Report_{fingerprint}.pdf"
    out_path = out_dir / filename
    out_path.write_bytes(pdf_bytes)
    print(f"Output path : {out_path}")
    print(f"PDF size    : {size:,} bytes")

    # Validity checks
    errors = []
    if not pdf_bytes[:5] == b"%PDF-":
        errors.append("FAIL: output does not start with %PDF header")
    else:
        print("CHECK       : %PDF header present")

    if size < 10_000:
        errors.append(f"FAIL: output size ({size:,} bytes) is below 10 KB threshold")
    else:
        print(f"CHECK       : size ({size:,} bytes) exceeds 10 KB threshold")

    if errors:
        for e in errors:
            print(e)
        sys.exit(1)

    print("All checks passed.")


if __name__ == "__main__":
    main()
