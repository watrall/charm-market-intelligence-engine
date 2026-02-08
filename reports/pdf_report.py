"""Assemble the CHARM PDF report using ReportLab flowables.

Exports: render_report_pdf.
"""

from __future__ import annotations

import io
from typing import Any

from reportlab.lib.units import inch
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
)

from reports.styles import (
    CONTENT_WIDTH,
    DARK_GRAY,
    FONT_FAMILY,
    FONT_FAMILY_BOLD,
    LIGHT_GRAY,
    MARGIN_BOTTOM,
    MARGIN_LEFT,
    MARGIN_RIGHT,
    MARGIN_TOP,
    MID_GRAY,
    PAGE_HEIGHT,
    PAGE_WIDTH,
    SP4,
    SP8,
    SP12,
    SP16,
    SP24,
    SP32,
    VERY_LIGHT_GRAY,
    get_styles,
    finding_table_style,
    skill_table_style,
)


# ---------------------------------------------------------------------------
# Page templates (cover vs body)
# ---------------------------------------------------------------------------

def _on_cover_page(canvas, doc):
    """No header/footer on cover page."""
    pass


def _on_body_page(canvas, doc):
    """Draw footer on every body page."""
    canvas.saveState()
    canvas.setFont(FONT_FAMILY, 8)
    canvas.setFillColor(LIGHT_GRAY)

    # Left footer
    canvas.drawString(
        MARGIN_LEFT,
        MARGIN_BOTTOM - 24,
        "CHARM Market Intelligence Report",
    )

    # Right footer: page number
    canvas.drawRightString(
        PAGE_WIDTH - MARGIN_RIGHT,
        MARGIN_BOTTOM - 24,
        f"Page {doc.page}",
    )

    # Optional: run date in center
    run_date = getattr(doc, "_charm_run_date", "")
    if run_date:
        canvas.drawCentredString(
            PAGE_WIDTH / 2,
            MARGIN_BOTTOM - 24,
            run_date,
        )

    canvas.restoreState()


def _build_doc(buffer: io.BytesIO, run_date: str = "") -> BaseDocTemplate:
    """Create a BaseDocTemplate with cover + body page templates."""
    body_frame = Frame(
        MARGIN_LEFT,
        MARGIN_BOTTOM,
        CONTENT_WIDTH,
        PAGE_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM,
        id="body",
    )

    cover_template = PageTemplate(
        id="cover",
        frames=[body_frame],
        onPage=_on_cover_page,
    )

    body_template = PageTemplate(
        id="body_tmpl",
        frames=[body_frame],
        onPage=_on_body_page,
    )

    doc = BaseDocTemplate(
        buffer,
        pagesize=(PAGE_WIDTH, PAGE_HEIGHT),
        leftMargin=MARGIN_LEFT,
        rightMargin=MARGIN_RIGHT,
        topMargin=MARGIN_TOP,
        bottomMargin=MARGIN_BOTTOM,
        title="CHARM Market Intelligence Report",
        author="CHARM Market Intelligence Engine",
    )
    doc.addPageTemplates([cover_template, body_template])
    doc._charm_run_date = run_date
    return doc


# ---------------------------------------------------------------------------
# Section builders (each returns a list of flowables)
# ---------------------------------------------------------------------------

def _cover_page(ctx: dict, ss) -> list:
    """Section 1: Cover page."""
    elements: list = []

    # Generous top spacing
    elements.append(Spacer(1, 2.2 * inch))

    # Title
    elements.append(Paragraph(ctx.get("title", "CHARM Market Intelligence Report"), ss["CoverTitle"]))
    elements.append(Spacer(1, SP16))

    # Subtitle: date range + run date
    date_range = ctx.get("date_range", "")
    run_date = ctx.get("run_date", "")
    sub_parts = []
    if date_range:
        sub_parts.append(f"Data period: {date_range}")
    if run_date:
        sub_parts.append(f"Generated: {run_date}")
    if sub_parts:
        elements.append(Paragraph("<br/>".join(sub_parts), ss["CoverSubtitle"]))

    # Filter scope
    filters = ctx.get("filters")
    if filters:
        scope_parts = []
        if filters.get("skills"):
            scope_parts.append(f"Skills: {', '.join(filters['skills'][:5])}")
        if filters.get("seniority"):
            scope_parts.append(f"Seniority: {', '.join(filters['seniority'])}")
        if scope_parts:
            elements.append(Spacer(1, SP8))
            elements.append(Paragraph(" | ".join(scope_parts), ss["CoverSubtitle"]))

    # Meta line
    fp = ctx.get("fingerprint", "")
    if fp:
        elements.append(Spacer(1, SP32))
        elements.append(Paragraph(f"Report fingerprint: {fp}", ss["CoverMeta"]))

    # Switch to body template for subsequent pages
    elements.append(NextPageTemplate("body_tmpl"))
    elements.append(PageBreak())
    return elements


def _executive_summary(ctx: dict, ss) -> list:
    """Section 2: Executive Summary. Exactly 5 bullets."""
    elements: list = []
    elements.append(Paragraph("Executive Summary", ss["H2"]))
    elements.append(Spacer(1, SP8))

    bullets = ctx.get("executive_summary", [])
    if not bullets:
        bullets = ["Not available in this run."]

    for bullet in bullets[:5]:
        elements.append(Paragraph(
            f"\u2022  {_esc(bullet)}", ss["Bullet"]
        ))

    elements.append(PageBreak())
    return elements


def _key_findings(ctx: dict, ss) -> list:
    """Section 3: Key Findings as a table."""
    elements: list = []
    elements.append(Paragraph("Key Findings", ss["H2"]))
    elements.append(Spacer(1, SP12))

    findings = ctx.get("key_findings", [])
    if not findings:
        elements.append(Paragraph("No findings data available in this run.", ss["Body"]))
        elements.append(PageBreak())
        return elements

    # Build 3-column grid of finding tiles
    cols = 3
    rows_data: list[list] = []
    row: list = []
    for finding in findings:
        cell_content = [
            Paragraph(str(finding["value"]), ss["FindingValue"]),
            Paragraph(str(finding["label"]), ss["FindingLabel"]),
        ]
        row.append(cell_content)
        if len(row) == cols:
            rows_data.append(row)
            row = []
    if row:
        while len(row) < cols:
            row.append([Paragraph("", ss["FindingLabel"])])
        rows_data.append(row)

    col_width = CONTENT_WIDTH / cols
    tbl = Table(rows_data, colWidths=[col_width] * cols)
    tbl.setStyle(finding_table_style())
    elements.append(tbl)

    elements.append(PageBreak())
    return elements


def _trends_and_signals(ctx: dict, ss) -> list:
    """Section 4: Trends & Signals. Top skills + emerging skills tables."""
    elements: list = []
    elements.append(Paragraph("Trends &amp; Signals", ss["H2"]))

    # Top skills table
    top_skills = ctx.get("top_skills", [])
    if top_skills:
        elements.append(Spacer(1, SP8))
        elements.append(Paragraph("Top Skills", ss["H3"]))
        elements.append(Spacer(1, SP8))

        header = [
            Paragraph("Rank", ss["TableHeader"]),
            Paragraph("Skill", ss["TableHeader"]),
            Paragraph("Mentions", ss["TableHeader"]),
        ]
        rows = [header]
        for i, (skill, count) in enumerate(top_skills, 1):
            rows.append([
                Paragraph(str(i), ss["TableCell"]),
                Paragraph(_esc(skill), ss["TableCell"]),
                Paragraph(str(count), ss["TableCell"]),
            ])

        tbl = Table(rows, colWidths=[0.6 * inch, CONTENT_WIDTH - 1.8 * inch, 1.2 * inch])
        tbl.setStyle(skill_table_style())
        elements.append(tbl)

    # Emerging skills table
    emerging = ctx.get("emerging_skills", [])
    if emerging:
        elements.append(Spacer(1, SP24))
        elements.append(Paragraph("Emerging Skills", ss["H3"]))
        elements.append(Spacer(1, SP8))

        header = [
            Paragraph("Rank", ss["TableHeader"]),
            Paragraph("Skill", ss["TableHeader"]),
            Paragraph("Mentions", ss["TableHeader"]),
        ]
        rows = [header]
        for i, (skill, count) in enumerate(emerging, len(top_skills) + 1):
            rows.append([
                Paragraph(str(i), ss["TableCell"]),
                Paragraph(_esc(skill), ss["TableCell"]),
                Paragraph(str(count), ss["TableCell"]),
            ])

        tbl = Table(rows, colWidths=[0.6 * inch, CONTENT_WIDTH - 1.8 * inch, 1.2 * inch])
        tbl.setStyle(skill_table_style())
        elements.append(tbl)

    elements.append(PageBreak())
    return elements


def _implications(ctx: dict, ss) -> list:
    """Section 5: Implications / Opportunities. Card-style layout."""
    elements: list = []
    elements.append(Paragraph("Implications &amp; Opportunities", ss["H2"]))
    elements.append(Spacer(1, SP12))

    items = ctx.get("implications", [])
    if not items:
        elements.append(Paragraph("No implications data available in this run.", ss["Body"]))
        elements.append(PageBreak())
        return elements

    for item in items:
        # Card: thin left border via a single-cell table
        card_content = []
        card_content.append(Paragraph(_esc(item.get("title", "")), ss["ImplicationTitle"]))
        card_content.append(Paragraph(_esc(item.get("explanation", "")), ss["ImplicationBody"]))
        why = item.get("why", "")
        if why:
            card_content.append(Paragraph(f"Why it matters: {_esc(why)}", ss["WhyItMatters"]))

        # Wrap in a table with left border
        from reportlab.platypus import TableStyle

        card_table = Table(
            [[card_content]],
            colWidths=[CONTENT_WIDTH - SP16],
        )
        card_table.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), SP12),
            ("RIGHTPADDING", (0, 0), (-1, -1), SP8),
            ("TOPPADDING", (0, 0), (-1, -1), SP8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), SP8),
            ("LINEBEFORESTARTCAP", (0, 0), (0, -1), 2, MID_GRAY),
            ("BACKGROUND", (0, 0), (-1, -1), VERY_LIGHT_GRAY),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        elements.append(card_table)
        elements.append(Spacer(1, SP12))

    elements.append(PageBreak())
    return elements


def _methods_and_governance(ctx: dict, ss) -> list:
    """Section 6: Methods & Governance."""
    elements: list = []
    elements.append(Paragraph("Methods &amp; Governance", ss["H2"]))
    elements.append(Spacer(1, SP12))

    methods = ctx.get("methods", {})

    # Data sources
    elements.append(Paragraph("Data Sources", ss["H3"]))
    for src in methods.get("data_sources", []):
        elements.append(Paragraph(f"\u2022  {_esc(src)}", ss["Bullet"]))
    elements.append(Spacer(1, SP8))

    # Approach
    elements.append(Paragraph("Approach", ss["H3"]))
    elements.append(Paragraph(_esc(methods.get("approach", "")), ss["Body"]))
    elements.append(Spacer(1, SP8))

    # Limitations
    elements.append(Paragraph("Limitations", ss["H3"]))
    for lim in methods.get("limitations", []):
        elements.append(Paragraph(f"\u2022  {_esc(lim)}", ss["Bullet"]))
    elements.append(Spacer(1, SP8))

    # Governance
    elements.append(Paragraph("Governance", ss["H3"]))
    elements.append(Paragraph(_esc(methods.get("governance", "")), ss["Body"]))

    elements.append(PageBreak())
    return elements


def _appendix(ctx: dict, ss) -> list:
    """Section 7: Appendix. Definitions + sources."""
    elements: list = []
    elements.append(Paragraph("Appendix: Definitions &amp; Sources", ss["H2"]))
    elements.append(Spacer(1, SP12))

    # Definitions
    elements.append(Paragraph("Definitions", ss["H3"]))
    elements.append(Spacer(1, SP8))

    defs = ctx.get("appendix_definitions", [])
    if defs:
        header = [
            Paragraph("Term", ss["TableHeader"]),
            Paragraph("Definition", ss["TableHeader"]),
        ]
        rows = [header]
        for d in defs:
            rows.append([
                Paragraph(_esc(d["term"]), ss["TableCell"]),
                Paragraph(_esc(d["definition"]), ss["TableCell"]),
            ])

        tbl = Table(rows, colWidths=[1.5 * inch, CONTENT_WIDTH - 1.5 * inch])
        tbl.setStyle(skill_table_style())
        elements.append(tbl)

    # Sources
    elements.append(Spacer(1, SP24))
    elements.append(Paragraph("Sources", ss["H3"]))
    elements.append(Spacer(1, SP8))
    elements.append(Paragraph(
        "Data collected from professional association job boards "
        "(ACRA, AAA) and industry PDF reports. Sources are categorized "
        "at a high level to protect individual listing URLs.",
        ss["Body"],
    ))

    # Top employers as a source reference
    top_emp = ctx.get("appendix_sources", [])
    if top_emp:
        elements.append(Spacer(1, SP8))
        elements.append(Paragraph("Employers represented in this dataset:", ss["Body"]))
        for emp in top_emp[:20]:
            name = emp[0] if isinstance(emp, (list, tuple)) else str(emp)
            elements.append(Paragraph(f"\u2022  {_esc(name)}", ss["Bullet"]))

    # Fingerprint + timestamp
    elements.append(Spacer(1, SP24))
    fp = ctx.get("fingerprint", "")
    run_date = ctx.get("run_date", "")
    meta_parts = []
    if fp:
        meta_parts.append(f"Report fingerprint: {fp}")
    if run_date:
        meta_parts.append(f"Processing timestamp: {run_date}")
    if meta_parts:
        elements.append(Paragraph(" | ".join(meta_parts), ss["Small"]))

    return elements


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape XML special characters for ReportLab Paragraph markup."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def render_report_pdf(context: dict) -> bytes:
    """Assemble the full PDF report from a context dict and return raw bytes."""
    buffer = io.BytesIO()
    ss = get_styles()
    run_date = context.get("run_date", "")
    doc = _build_doc(buffer, run_date=run_date)

    # Assemble flowables in section order
    story: list = []
    story.extend(_cover_page(context, ss))
    story.extend(_executive_summary(context, ss))
    story.extend(_key_findings(context, ss))
    story.extend(_trends_and_signals(context, ss))
    story.extend(_implications(context, ss))
    story.extend(_methods_and_governance(context, ss))
    story.extend(_appendix(context, ss))

    doc.build(story)
    return buffer.getvalue()
