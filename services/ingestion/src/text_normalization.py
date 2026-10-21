"""
State-specific HTML normalization before markdown conversion.

Different state legislature sites encode insertions/deletions differently.
Handlers in this module mutate BeautifulSoup trees in place so downstream
text extraction produces more consistent markup.
"""

from __future__ import annotations

import re
from typing import Callable, Dict

from bs4 import BeautifulSoup


def _noop(_soup: BeautifulSoup) -> None:
    """Fallback handler for states without custom normalization rules."""
    return


def _normalize_ny(soup: BeautifulSoup) -> None:
    """Rewrite NY deletion tags to explicit [DEL] markers for stable downstream parsing."""
    for element in soup.find_all(["strike", "s", "del"]):
        text = element.get_text(strip=True)
        element.replace_with(f"[DEL]{text}[/DEL]")


def _normalize_wv(soup: BeautifulSoup) -> None:
    """Convert WV class-based strikeout spans into semantic <del> tags."""
    # WV strikeouts are often class-based in <style> blocks rather than <del> tags.
    strike_classes = set()
    for style in soup.find_all("style"):
        for match in re.finditer(r"\.(\w+)\s*\{[^}]*line-through", style.get_text(), re.I):
            strike_classes.add(match.group(1))

    for cls in strike_classes:
        for span in soup.find_all("span", class_=cls):
            span.name = "del"
            span.attrs = {}


def _normalize_az(soup: BeautifulSoup) -> None:
    """Map AZ deletion spans to markdown strikeout syntax."""
    for span in soup.find_all("span", class_="O"):
        text = span.get_text(strip=True)
        span.replace_with(f"~~{text}~~")


def _normalize_ca(soup: BeautifulSoup) -> None:
    """Map CA deletion tags to markdown strikeout syntax."""
    for element in soup.find_all(["strike", "s", "del"]):
        text = element.get_text(strip=True)
        element.replace_with(f"~~{text}~~")


def _normalize_de_or_nh(soup: BeautifulSoup) -> None:
    """Convert DE/NH CSS strikeout classes into inline markdown strikeout text."""
    strike_classes = set()
    for style in soup.find_all("style"):
        for match in re.finditer(
            r"\.(\S+)\s*\{[^}]*text-decoration\s*:\s*line-through",
            style.get_text(),
            re.I,
        ):
            strike_classes.add(match.group(1))

    for cls in strike_classes:
        for span in soup.find_all("span", class_=cls):
            text = span.get_text(strip=True)
            span.replace_with(f"~~{text}~~")


def _normalize_il(soup: BeautifulSoup) -> None:
    """Handle IL-specific deletion/insertion markers and flatten table-based bill text."""
    for element in soup.find_all("strike"):
        text = element.get_text(strip=True)
        element.replace_with(f"[DEL]{text}[/DEL]")

    for element in soup.find_all("u"):
        text = element.get_text(strip=True)
        element.replace_with(f"[INS]{text}[/INS]")

    for element in soup.find_all("code"):
        # IL often wraps visible body text in <code>; keep text but drop wrapper.
        element.unwrap()

    for table in soup.find_all("table"):
        lines = []
        for row in table.find_all("tr"):
            cell = row.find("td", class_="xsl")
            if cell:
                text = cell.get_text(" ", strip=True)
                if text:
                    lines.append(text)

        for line in lines:
            paragraph = soup.new_tag("p")
            paragraph.string = line
            table.insert_before(paragraph)

        # After flattening rows into paragraphs, the source table can be removed.
        table.decompose()


def _normalize_nj(soup: BeautifulSoup) -> None:
    """Preserve NJ bracket-style deletions by converting bold bracket markers."""
    # NJ pages sometimes mark deletions with bolded brackets; preserve that marker.
    for bold in soup.find_all("b"):
        text = bold.get_text(strip=True)
        if text == "[":
            bold.replace_with("[~~")
        elif text == "]":
            bold.replace_with("~~]")


def _normalize_sc(soup: BeautifulSoup) -> None:
    """Handle SC inline style-based insertions and deletions."""
    for span in soup.find_all("span"):
        style = span.get("style", "").lower()

        text = span.get_text(strip=True)
        if not text:
            continue

        if "line-through" in style:
            span.replace_with(f"[DEL]{text}[/DEL]")
        elif "underline" in style:
            span.replace_with(f"[INS]{text}[/INS]")


def _normalize_tx(soup: BeautifulSoup) -> None:
    """Convert TX table-based bills into paragraphs while preserving inline markup."""
    for table in soup.find_all("table"):
        lines = []
        for tr in table.find_all("tr"):
            cells = []

            for td in tr.find_all("td"):
                html = td.decode_contents(formatter="html").strip()

                if html and html != "&nbsp;" and html != "&#xA0;":
                    cells.append(html)

            if cells:
                line = cells[-1]
                lines.append(line)

        for line in lines:
            p = soup.new_tag("p")
            p.append(BeautifulSoup(line, "html.parser"))
            table.insert_before(p)

        table.decompose()


_STATE_HANDLERS: Dict[str, Callable[[BeautifulSoup], None]] = {
    "NY": _normalize_ny,
    "WV": _normalize_wv,
    "VA": _noop,
    "AZ": _normalize_az,
    "CA": _normalize_ca,
    "DE": _normalize_de_or_nh,
    "NH": _normalize_de_or_nh,
    "IL": _normalize_il,
    "NJ": _normalize_nj,
    "TX": _normalize_tx,
    "SC": _normalize_sc,
}


def normalize_strikethroughs(soup: BeautifulSoup, state: str) -> None:
    """Apply the state normalization routine in place, defaulting to no-op."""
    # Handlers mutate soup in place; caller can immediately serialize.
    handler = _STATE_HANDLERS.get((state or "").upper(), _noop)
    handler(soup)
