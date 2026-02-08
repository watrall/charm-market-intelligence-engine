"""Shared dashboard header with PDF download button."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import streamlit as st

# Lucide "download" icon, inline SVG (decorative)
_DOWNLOAD_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" '
    'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" '
    'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
    '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
    '<polyline points="7 10 12 15 17 10"/>'
    '<line x1="12" y1="15" x2="12" y2="3"/></svg>'
)


def _header_css():
    """Inject minimal CSS for the header strip (once per session)."""
    if st.session_state.get("_header_css_loaded"):
        return
    st.markdown(
        """
        <style>
        .charm-header-strip {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
            padding: 4px 0 8px 0;
        }
        .charm-dl-icon {
            display: inline-flex;
            align-items: center;
            color: #333;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_header_css_loaded"] = True


@st.cache_data(show_spinner="Generating report\u2026")
def _cached_report_bytes(fingerprint: str, proc_dir_str: str) -> bytes:
    """Generate and cache report PDF bytes keyed by fingerprint."""
    from reports.context import build_report_context
    from reports.pdf_report import render_report_pdf

    ctx = build_report_context(Path(proc_dir_str))
    return render_report_pdf(ctx)


def render_header(proc_dir: Path | str) -> None:
    """Render the download-report header strip.

    Call this at the top of every dashboard page/section entrypoint.
    """
    _header_css()
    proc_dir = Path(proc_dir)

    # Check if source data exists
    has_data = (proc_dir / "jobs.csv").exists() or (proc_dir / "analysis.json").exists()

    # Build layout: spacer + icon + download button
    cols = st.columns([0.82, 0.18])

    with cols[1]:
        if has_data:
            from reports.context import compute_report_fingerprint

            fp = compute_report_fingerprint(proc_dir)
            pdf_bytes = _cached_report_bytes(fp, str(proc_dir))

            # Date for filename
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            filename = f"CHARM_Report_{date_str}_{fp[:8]}.pdf"

            # Icon + button in a compact layout
            st.markdown(
                f'<div class="charm-header-strip">'
                f'<span class="charm-dl-icon">{_DOWNLOAD_SVG}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.download_button(
                label="Download report",
                data=pdf_bytes,
                file_name=filename,
                mime="application/pdf",
                type="primary",
                use_container_width=True,
            )
        else:
            st.caption("No data available for report.")
