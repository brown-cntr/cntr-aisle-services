"""Format-aware cleanup for extracted bill text.

Runs after MIME extraction to strip PDF layout furniture (line-number gutters)
that structured HTML/XML never carries. Each removal is validated before it fires
and guarded against over-removal, so statutory text is preserved.
"""

from __future__ import annotations

import re
from typing import List, Optional

# PDF and legacy office formats carry layout furniture; HTML (1) / XML do not.
_LAYOUT_FURNITURE_MIME_IDS = frozenset({2, 3, 4, 5})

# Revert a step that removes more than this fraction of non-whitespace chars.
_MAX_SAFE_REMOVAL = 0.40
# A leading gutter line: optional markdown prefix, small integer, then content.
_GUTTER_LINE = re.compile(r"^(?P<prefix>(?:#{1,6}|[*>-]+)?\s*)(?P<num>\d{1,3})[ \t]+(?P<rest>\S.*)$")
_BARE_SMALL_INT = re.compile(r"^(\d{1,3})$")


def _looks_like_gutter(numbers: List[int]) -> bool:
    """True if leading integers march 1,2,3 with page resets (a real gutter)."""
    if len(numbers) < 4:
        return False
    steps = ordered = 0
    for prev, cur in zip(numbers, numbers[1:]):
        steps += 1
        if cur == prev + 1 or (cur <= prev and cur <= 3):  # increment or page reset
            ordered += 1
    return steps > 0 and ordered / steps >= 0.6


def strip_line_number_gutters(text: str) -> str:
    """Strip a per-page leading line-number column, only if one is detected."""
    lines = text.split("\n")
    numbers = [int(m.group("num")) for line in lines if (m := _GUTTER_LINE.match(line))]
    if not _looks_like_gutter(numbers):
        return text

    cleaned: List[str] = []
    for line in lines:
        m = _GUTTER_LINE.match(line)
        if m:
            cleaned.append(f"{m.group('prefix')}{m.group('rest')}")
        elif not _BARE_SMALL_INT.match(line.strip()):  # drop a wrapped gutter number
            cleaned.append(line)
    return "\n".join(cleaned)


def dehyphenate(text: str) -> str:
    """Rejoin lowercase words split by a hyphen at a line break (``inter-\\nstate``)."""
    return re.sub(r"([a-z])-\n([a-z])", r"\1\2", text)


def _nonspace_len(text: str) -> int:
    return len(re.sub(r"\s+", "", text))


def _guard(original: str, candidate: str) -> str:
    """Keep ``candidate`` unless it removed more than _MAX_SAFE_REMOVAL of the text."""
    base = _nonspace_len(original)
    if base and _nonspace_len(candidate) < base * (1 - _MAX_SAFE_REMOVAL):
        return original
    return candidate


def clean_document(text: str, mime_id: int, state: Optional[str] = None) -> str:
    """Clean extracted text for its source format and state.

    PDF-family formats get gutter removal; HTML/XML get only de-hyphenation,
    preserving the per-state HTML normalization in ``text_normalization``.
    """
    if not text:
        return text
    if mime_id in _LAYOUT_FURNITURE_MIME_IDS:
        text = _guard(text, strip_line_number_gutters(text))
    return dehyphenate(text)
