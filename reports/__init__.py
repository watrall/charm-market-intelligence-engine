"""CHARM PDF report generation package.

Exports: build_report_context, compute_report_fingerprint, render_report_pdf.
"""

from reports.context import build_report_context, compute_report_fingerprint
from reports.pdf_report import render_report_pdf

__all__ = [
    "build_report_context",
    "compute_report_fingerprint",
    "render_report_pdf",
]
