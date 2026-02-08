"""Shared dashboard header with PDF download button."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


def _header_css():
    """Inject CSS for the sidebar report download button (once per session)."""
    if st.session_state.get("_sidebar_report_css_loaded_v6"):
        return
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] div[data-testid="stDownloadButton"] > button {
            display: flex;
            align-items: center;
            justify-content: center;
            flex-direction: row;
            gap: 0.42rem;
            white-space: nowrap;
            width: 100%;
            box-sizing: border-box;
        }
        section[data-testid="stSidebar"] div[data-testid="stDownloadButton"] > button > div {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            white-space: nowrap;
        }
        section[data-testid="stSidebar"] div[data-testid="stDownloadButton"] > button::before {
            content: "";
            display: inline-block;
            width: 16px;
            height: 16px;
            flex: 0 0 16px;
            background-image: url("https://cdn.jsdelivr.net/npm/lucide-static@0.321.0/icons/file-down.svg");
            background-size: 16px 16px;
            background-repeat: no-repeat;
            background-position: center;
            filter: brightness(0) saturate(100%) invert(100%);
        }
        section[data-testid="stSidebar"] div[data-testid="stDownloadButton"] > button > div::before {
            content: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_sidebar_report_css_loaded_v6"] = True


def _set_browser_title(title: str = "CHARM") -> None:
    """Force browser tab title for Streamlit multipage surfaces."""
    components.html(
        f"<script>window.parent.document.title = {title!r};</script>",
        height=0,
        width=0,
    )


@st.cache_data(show_spinner="Generating report\u2026")
def _cached_report_bytes(fingerprint: str, proc_dir_str: str) -> bytes:
    """Generate and cache report PDF bytes keyed by fingerprint."""
    from reports.context import build_report_context
    from reports.pdf_report import render_report_pdf

    ctx = build_report_context(Path(proc_dir_str))
    return render_report_pdf(ctx)


def render_header(proc_dir: Path | str) -> None:
    """Render the report download button in the sidebar.

    Call this at the top of every dashboard page/section entrypoint.
    """
    _header_css()
    _set_browser_title("CHARM")
    proc_dir = Path(proc_dir)

    # Check if source data exists
    has_data = (proc_dir / "jobs.csv").exists() or (proc_dir / "analysis.json").exists()

    with st.sidebar:
        if has_data:
            from reports.context import compute_report_fingerprint

            fp = compute_report_fingerprint(proc_dir)
            pdf_bytes = _cached_report_bytes(fp, str(proc_dir))

            # Date for filename
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            filename = f"CHARM_Report_{date_str}_{fp[:8]}.pdf"

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
