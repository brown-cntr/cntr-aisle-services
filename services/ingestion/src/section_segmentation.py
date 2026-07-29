"""Split cleaned bill text into structured sections.

Detects the section markers U.S. bills use at the start of a line (``Section 1.``,
``Sec. 2.``, ``SECTION 3``) and returns an ordered list of sections. Text before
the first marker is a ``preamble`` section; if no markers exist the whole document
is one section. Non-destructive: joining the section bodies reproduces the input.
"""

from __future__ import annotations

import re
from typing import List, Optional, TypedDict

# Start-of-line only (so "under Sec. 5 of the Act" mid-sentence doesn't match),
# after optional markdown; number may be code-style ("1-101", "12A").
_SECTION_MARKER = re.compile(
    r"^[\s>#*_-]*"
    r"(?:SECTION|Section|Sec)\.?\s+"
    r"(?P<number>\d+[A-Za-z]?(?:[-.][0-9A-Za-z]+)*)"
    r"(?=[.\s]|$)",
)


class Section(TypedDict):
    """One segmented section of a bill."""

    number: Optional[str]  # section number, or None for the preamble
    heading: str           # the marker line (or "" for the preamble)
    text: str              # the section body, including its heading line


def _is_section_header(line: str) -> Optional[str]:
    """Return the section number if ``line`` begins a section, else ``None``."""
    m = _SECTION_MARKER.match(line)
    return m.group("number") if m else None


def segment_sections(text: str) -> List[Section]:
    """Split ``text`` into an ordered list of :class:`Section` records."""
    if not text or not text.strip():
        return []

    lines = text.split("\n")
    sections: List[Section] = []
    current: Section = {"number": None, "heading": "", "text": ""}
    buffer: List[str] = []

    def flush() -> None:
        current["text"] = "\n".join(buffer).strip("\n")
        if current["text"] or current["number"] is not None:
            sections.append(current)

    for line in lines:
        number = _is_section_header(line)
        if number is not None:
            flush()
            current = {"number": number, "heading": line.strip(), "text": ""}
            buffer = [line]
        else:
            buffer.append(line)
    flush()

    return sections
