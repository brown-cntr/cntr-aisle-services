"""Format-aware cleanup for extracted bill text.

Runs after MIME extraction to strip PDF layout furniture (line-number gutters,
running headers/footers, page numbers) that structured HTML/XML never carries.
Each removal is validated before it fires and guarded against over-removal, so
statutory text is preserved.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Callable, Dict, List, Optional

# PDF and legacy office formats carry layout furniture; HTML (1) / XML do not.
_LAYOUT_FURNITURE_MIME_IDS = frozenset({2, 3, 4, 5})

# A line that is only a (optionally dash/asterisk-wrapped) page number.
_PAGE_NUMBER_LINE = re.compile(r"^[\s*_-]*\d{1,4}[\s*_-]*$")
# A "Page N of M" footer (also "Page of M" after inline-gutter removal).
_PAGE_OF_LINE = re.compile(r"(?i)^\**\s*page\s*(\d{1,4}\s+)?of\s+\d{1,4}\s*\**$")
# A dash-wrapped page number inside a running footer ("**-1-**", "- 12 -").
_EMBEDDED_PAGE_NUMBER = re.compile(r"[-–]\s*\d{1,4}\s*[-–]")
# Longest a line can be and still count as a header/footer (not statutory text).
_FOOTER_MAX_LEN = 60
# Revert a step that removes more than this fraction of non-whitespace chars.
_MAX_SAFE_REMOVAL = 0.40
# Subsection/list markers to never treat as furniture: "(a)", "(1)", "1.", "iv.".
_SUBSECTION_MARKER = re.compile(r"^[(\[]?[A-Za-z0-9]{1,4}[)\].]?$")
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


def strip_page_markers(text: str) -> str:
    """Drop page numbers and "Page N of M" / dash-wrapped footers by shape.

    Pattern-verified (never statutory text), so this runs unguarded and cleans
    even short bills. The dash-wrapped rule is short-line only, so a long citation
    like "26-1-119.5" is kept.
    """
    kept: List[str] = []
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            kept.append(line)
        elif _PAGE_NUMBER_LINE.match(stripped) or _PAGE_OF_LINE.match(stripped):
            continue
        elif len(stripped) <= _FOOTER_MAX_LEN and _EMBEDDED_PAGE_NUMBER.search(stripped):
            continue
        else:
            kept.append(line)
    return "\n".join(kept)


def remove_repeated_furniture(text: str, min_repeats: int = 3, max_len: int = 90) -> str:
    """Drop short lines that repeat verbatim >= min_repeats times (guarded by caller).

    Exact matching keeps distinct appropriations items ("$153,663,700 from General
    Fund") that differ only by amount; subsection markers are protected.
    """
    lines = text.split("\n")

    def canon(line: str) -> str:
        return " ".join(line.split())

    counts: Counter[str] = Counter(
        canon(s) for line in lines if (s := line.strip()) and not _SUBSECTION_MARKER.match(s)
    )
    cleaned: List[str] = []
    for line in lines:
        stripped = line.strip()
        if (
            stripped
            and len(stripped) <= max_len
            and not _SUBSECTION_MARKER.match(stripped)
            and counts[canon(stripped)] >= min_repeats
        ):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def dehyphenate(text: str) -> str:
    """Rejoin lowercase words split by a hyphen at a line break (``inter-\\nstate``)."""
    return re.sub(r"([a-z])-\n([a-z])", r"\1\2", text)


# Inline line-number gutters: CO/LA/MD (and some UT/FL) render the counter mid-line,
# e.g. "...EDUCATION. 15 7-2201. 16 IN THIS SUBTITLE 17 ...". The counter increments;
# prose numbers do not, so we only remove tokens with a sequential neighbour.
_INLINE_GUTTER_TOKEN = re.compile(r"(?<=\S )(\d{1,3})(?= \S)")
_INLINE_GUTTER_WINDOW = 6
_INLINE_GUTTER_MIN_TOKENS = 10
_INLINE_GUTTER_MIN_RATIO = 0.4
_MULTISPACE = re.compile(r"[ \t]{2,}")


def strip_inline_gutters(text: str) -> str:
    """Remove mid-line line numbers, keeping only tokens with a +/-1 neighbour."""
    toks = [(m.start(), m.end(), int(m.group(1))) for m in _INLINE_GUTTER_TOKEN.finditer(text)]
    if len(toks) < _INLINE_GUTTER_MIN_TOKENS:
        return text

    vals = [v for _, _, v in toks]
    gutter_idx = {
        i for i, v in enumerate(vals)
        if (v - 1) in vals[max(0, i - _INLINE_GUTTER_WINDOW):i]
        or (v + 1) in vals[i + 1:i + 1 + _INLINE_GUTTER_WINDOW]
    }
    if len(gutter_idx) / len(toks) < _INLINE_GUTTER_MIN_RATIO:
        return text

    pieces: List[str] = []
    last = 0
    for i, (start, end, _v) in enumerate(toks):
        if i in gutter_idx:
            pieces.append(text[last:start])
            last = end + 1  # also drop the token's trailing space
    pieces.append(text[last:])
    joined = "".join(pieces)
    return "\n".join(_MULTISPACE.sub(" ", line) for line in joined.split("\n"))


# markdownify splits superscript ordinals ("76<sup>th</sup>" -> "76 [th]").
_SPLIT_ORDINAL = re.compile(r"(\d+) \[(st|nd|rd|th)\]")


def rejoin_split_ordinals(text: str) -> str:
    """Rejoin ordinals split by markdownify: ``76 [th]`` -> ``76th``."""
    return _SPLIT_ORDINAL.sub(r"\1\2", text)


def _nonspace_len(text: str) -> int:
    return len(re.sub(r"\s+", "", text))


def _guard(original: str, candidate: str) -> str:
    """Keep ``candidate`` unless it removed more than _MAX_SAFE_REMOVAL of the text."""
    base = _nonspace_len(original)
    if base and _nonspace_len(candidate) < base * (1 - _MAX_SAFE_REMOVAL):
        return original
    return candidate


# Per-state cleaners, keyed on 2-letter code, for quirks the generic passes miss.
# FL: bill-id header ("CS/HB 693 2026") and draft doc-code ("hb693-01-c1") that
# repeat too few times on short bills to trip the frequency rule.
_FL_HEADER = re.compile(r"(?i)^(?:CS/)*[HS]B\s*\d+\s+\d{4}$")
_FL_DOCCODE = re.compile(r"(?i)^[hs][bcjmr]\d+-\d+-[a-z]\d+$")


def _clean_fl(text: str) -> str:
    """Florida: drop the bill-id running header and draft doc-code footer."""
    return "\n".join(
        line for line in text.split("\n")
        if not (_FL_HEADER.match(line.strip()) or _FL_DOCCODE.match(line.strip()))
    )


_STATE_CLEANERS: Dict[str, Callable[[str], str]] = {
    "FL": _clean_fl,
}


def _apply_state_cleaners(text: str, state: Optional[str]) -> str:
    """Run the registered cleaner for ``state`` (if any), guarded like the rest."""
    cleaner = _STATE_CLEANERS.get(state.strip().upper()) if state else None
    return _guard(text, cleaner(text)) if cleaner else text


def clean_document(text: str, mime_id: int, state: Optional[str] = None) -> str:
    """Clean extracted text for its source format and state.

    PDF-family formats get gutter (leading + inline), page-marker, and furniture
    removal; HTML/XML get only ordinal/state fixes and de-hyphenation, preserving
    the per-state HTML normalization in ``text_normalization``.
    """
    if not text:
        return text
    if mime_id in _LAYOUT_FURNITURE_MIME_IDS:
        text = _guard(text, strip_line_number_gutters(text))
        text = _guard(text, strip_inline_gutters(text))
        text = strip_page_markers(text)  # pattern-verified -> unguarded
        text = _guard(text, remove_repeated_furniture(text))
    text = rejoin_split_ordinals(text)
    text = _apply_state_cleaners(text, state)
    return dehyphenate(text)
