"""Report-wide style system: fonts, paragraph styles, table styles, spacing.

Uses Inter (bundled TTF) as the primary sans-serif family.
Falls back to Helvetica if Inter cannot be loaded.
"""

from __future__ import annotations

import logging
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.styles import ParagraphStyle, StyleSheet1
from reportlab.lib.units import inch, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spacing scale (pt), required by spec
# ---------------------------------------------------------------------------
SP4 = 4
SP8 = 8
SP12 = 12
SP16 = 16
SP24 = 24
SP32 = 32

# ---------------------------------------------------------------------------
# Page setup: US Letter, portrait
# ---------------------------------------------------------------------------
PAGE_WIDTH = 8.5 * inch
PAGE_HEIGHT = 11 * inch
MARGIN_LEFT = 0.75 * inch
MARGIN_RIGHT = 0.75 * inch
MARGIN_TOP = 0.85 * inch
MARGIN_BOTTOM = 0.85 * inch
CONTENT_WIDTH = PAGE_WIDTH - MARGIN_LEFT - MARGIN_RIGHT

# ---------------------------------------------------------------------------
# Colors (grayscale only; charts are the sole exception)
# ---------------------------------------------------------------------------
BLACK = colors.HexColor("#000000")
DARK_GRAY = colors.HexColor("#333333")
MID_GRAY = colors.HexColor("#666666")
LIGHT_GRAY = colors.HexColor("#CCCCCC")
VERY_LIGHT_GRAY = colors.HexColor("#F2F2F2")
WHITE = colors.HexColor("#FFFFFF")

# Chart-only muted palette (used exclusively in charts.py)
CHART_COLORS = [
    colors.HexColor("#4C78A8"),
    colors.HexColor("#72B7B2"),
    colors.HexColor("#E59C50"),
    colors.HexColor("#8B8B8B"),
    colors.HexColor("#AF8DC3"),
]

# ---------------------------------------------------------------------------
# Font registration
# ---------------------------------------------------------------------------
_FONT_DIR = Path(__file__).resolve().parent / "fonts"
FONT_FAMILY = "Helvetica"  # fallback default
FONT_FAMILY_BOLD = "Helvetica-Bold"


def _register_inter() -> bool:
    """Try to register Inter TTF. Returns True on success."""
    regular = _FONT_DIR / "Inter-Regular.ttf"
    semibold = _FONT_DIR / "Inter-SemiBold.ttf"
    if not regular.exists() or not semibold.exists():
        logger.info("Inter font files not found at %s; using Helvetica fallback.", _FONT_DIR)
        return False
    try:
        pdfmetrics.registerFont(TTFont("Inter", str(regular)))
        pdfmetrics.registerFont(TTFont("Inter-SemiBold", str(semibold)))
        pdfmetrics.registerFontFamily(
            "Inter",
            normal="Inter",
            bold="Inter-SemiBold",
        )
        return True
    except Exception:
        logger.warning("Failed to register Inter font; using Helvetica.", exc_info=True)
        return False


# Run registration at import time
if _register_inter():
    FONT_FAMILY = "Inter"
    FONT_FAMILY_BOLD = "Inter-SemiBold"


# ---------------------------------------------------------------------------
# Paragraph styles
# ---------------------------------------------------------------------------

def get_styles() -> StyleSheet1:
    """Return the canonical report stylesheet."""
    ss = StyleSheet1()

    # Base body
    ss.add(ParagraphStyle(
        name="Body",
        fontName=FONT_FAMILY,
        fontSize=10.5,
        leading=14,
        spaceBefore=SP4,
        spaceAfter=SP8,
        textColor=DARK_GRAY,
    ))

    # Cover title
    ss.add(ParagraphStyle(
        name="CoverTitle",
        fontName=FONT_FAMILY_BOLD,
        fontSize=28,
        leading=34,
        alignment=TA_CENTER,
        textColor=BLACK,
        spaceAfter=SP16,
    ))

    # Cover subtitle
    ss.add(ParagraphStyle(
        name="CoverSubtitle",
        fontName=FONT_FAMILY,
        fontSize=13,
        leading=18,
        alignment=TA_CENTER,
        textColor=MID_GRAY,
        spaceAfter=SP8,
    ))

    # Cover meta
    ss.add(ParagraphStyle(
        name="CoverMeta",
        fontName=FONT_FAMILY,
        fontSize=9,
        leading=12,
        alignment=TA_CENTER,
        textColor=LIGHT_GRAY,
        spaceAfter=SP4,
    ))

    # Section headers
    ss.add(ParagraphStyle(
        name="H2",
        fontName=FONT_FAMILY_BOLD,
        fontSize=17,
        leading=22,
        spaceBefore=SP24,
        spaceAfter=SP12,
        textColor=BLACK,
    ))

    # Sub-headers
    ss.add(ParagraphStyle(
        name="H3",
        fontName=FONT_FAMILY_BOLD,
        fontSize=12.5,
        leading=16,
        spaceBefore=SP16,
        spaceAfter=SP8,
        textColor=DARK_GRAY,
    ))

    # Small / meta text
    ss.add(ParagraphStyle(
        name="Small",
        fontName=FONT_FAMILY,
        fontSize=8.5,
        leading=11,
        textColor=MID_GRAY,
        spaceAfter=SP4,
    ))

    # Bullet point style
    ss.add(ParagraphStyle(
        name="Bullet",
        fontName=FONT_FAMILY,
        fontSize=10.5,
        leading=14,
        leftIndent=18,
        bulletIndent=6,
        spaceBefore=SP4,
        spaceAfter=SP4,
        textColor=DARK_GRAY,
    ))

    # Key finding value (large, bold, in table cells)
    ss.add(ParagraphStyle(
        name="FindingValue",
        fontName=FONT_FAMILY_BOLD,
        fontSize=18,
        leading=22,
        alignment=TA_CENTER,
        textColor=BLACK,
        spaceAfter=SP4,
    ))

    # Key finding label
    ss.add(ParagraphStyle(
        name="FindingLabel",
        fontName=FONT_FAMILY,
        fontSize=9,
        leading=12,
        alignment=TA_CENTER,
        textColor=MID_GRAY,
    ))

    # Implication title
    ss.add(ParagraphStyle(
        name="ImplicationTitle",
        fontName=FONT_FAMILY_BOLD,
        fontSize=11,
        leading=14,
        textColor=BLACK,
        spaceBefore=SP8,
        spaceAfter=SP4,
    ))

    # Implication body
    ss.add(ParagraphStyle(
        name="ImplicationBody",
        fontName=FONT_FAMILY,
        fontSize=10,
        leading=13.5,
        textColor=DARK_GRAY,
        spaceAfter=SP4,
    ))

    # "Why it matters" style
    ss.add(ParagraphStyle(
        name="WhyItMatters",
        fontName=FONT_FAMILY,
        fontSize=9,
        leading=12,
        textColor=MID_GRAY,
        spaceAfter=SP8,
    ))

    # Footer
    ss.add(ParagraphStyle(
        name="Footer",
        fontName=FONT_FAMILY,
        fontSize=8,
        leading=10,
        textColor=LIGHT_GRAY,
    ))

    # Footer right-aligned
    ss.add(ParagraphStyle(
        name="FooterRight",
        fontName=FONT_FAMILY,
        fontSize=8,
        leading=10,
        textColor=LIGHT_GRAY,
        alignment=TA_RIGHT,
    ))

    # Table header cell
    ss.add(ParagraphStyle(
        name="TableHeader",
        fontName=FONT_FAMILY_BOLD,
        fontSize=9.5,
        leading=12,
        textColor=DARK_GRAY,
    ))

    # Table body cell
    ss.add(ParagraphStyle(
        name="TableCell",
        fontName=FONT_FAMILY,
        fontSize=9.5,
        leading=12,
        textColor=DARK_GRAY,
    ))

    return ss


# ---------------------------------------------------------------------------
# Table style helpers
# ---------------------------------------------------------------------------

def skill_table_style():
    """Return a clean, grayscale TableStyle for skills tables."""
    from reportlab.platypus import TableStyle

    return TableStyle([
        # Header row
        ("BACKGROUND", (0, 0), (-1, 0), VERY_LIGHT_GRAY),
        ("FONTNAME", (0, 0), (-1, 0), FONT_FAMILY_BOLD),
        ("FONTSIZE", (0, 0), (-1, 0), 9.5),
        ("TEXTCOLOR", (0, 0), (-1, 0), DARK_GRAY),
        # Body
        ("FONTNAME", (0, 1), (-1, -1), FONT_FAMILY),
        ("FONTSIZE", (0, 1), (-1, -1), 9.5),
        ("TEXTCOLOR", (0, 1), (-1, -1), DARK_GRAY),
        # Grid / lines
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, LIGHT_GRAY),
        ("LINEBELOW", (0, 1), (-1, -2), 0.25, LIGHT_GRAY),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, LIGHT_GRAY),
        # Padding
        ("TOPPADDING", (0, 0), (-1, -1), SP4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), SP4),
        ("LEFTPADDING", (0, 0), (-1, -1), SP8),
        ("RIGHTPADDING", (0, 0), (-1, -1), SP8),
        # Alignment
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])


def finding_table_style():
    """Return a tile-like TableStyle for the key findings grid (grayscale only)."""
    from reportlab.platypus import TableStyle

    return TableStyle([
        # Light separator lines between cells
        ("LINEAFTER", (0, 0), (-2, -1), 0.5, LIGHT_GRAY),
        ("LINEBELOW", (0, 0), (-1, -2), 0.5, LIGHT_GRAY),
        # Padding for tile feel
        ("TOPPADDING", (0, 0), (-1, -1), SP12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), SP12),
        ("LEFTPADDING", (0, 0), (-1, -1), SP12),
        ("RIGHTPADDING", (0, 0), (-1, -1), SP12),
        # Alignment
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ])
