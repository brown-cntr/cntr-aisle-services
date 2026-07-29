"""Reconcile OpenStates and LegiScan bill records into one deduplicated set.

The two sources describe the same bill with different identifiers ("HR 1234" vs
"HB1234") and slightly different dates. Matching pairs merge into one Bill with
both ids and source="both".
"""

from __future__ import annotations

import re
from datetime import date, datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from shared.models.bill import Bill

# (pattern, replacement, is_regex). Order matters: each rule sees prior output.
_OPEN_ID_REPLACEMENTS: Tuple[Tuple[str, str, bool], ...] = (
    ("HRES ", "HR", False),
    ("S ", "SB", False),
    ("SRES", "SR", False),
    ("HR", "HB", False),
    ("HCONRES", "HCR", False),
    ("K ", "K00", False),
    (r"\d{2}-", "", True),
    ("SJ ", "SJR", False),
    (" ", "", False),
)

_LEGI_ID_REPLACEMENTS: Tuple[Tuple[str, str, bool], ...] = (
    ("0000", "", False),
    ("SF00", "SF", False),
    ("000", "", False),
    ("S ", "SB", False),
    ("HR", "HB", False),
    ("A00", "A", False),
    ("A0", "A", False),
    ("H0", "H", False),
    ("HCR00", "HCR", False),
)


def _apply_replacements(value: str, rules: Tuple[Tuple[str, str, bool], ...]) -> str:
    """Apply ordered string/regex replacement rules to one identifier."""
    result = value or ""
    for pattern, replacement, is_regex in rules:
        if is_regex:
            result = re.sub(pattern, replacement, result)
        else:
            result = result.replace(pattern, replacement)
    return result


def normalize_openstates_identifier(identifier: str) -> str:
    """Normalize an OpenStates identifier (e.g. ``"HR 1234"``) for matching."""
    return _apply_replacements((identifier or "").strip(), _OPEN_ID_REPLACEMENTS)


def normalize_legiscan_bill_number(bill_number: str) -> str:
    """Normalize a LegiScan bill_number (e.g. ``"HR0001234"``) for matching."""
    return _apply_replacements((bill_number or "").strip(), _LEGI_ID_REPLACEMENTS)


def combined_identifier(normalized_number: str, state: str, when: object) -> str:
    """Build the canonical ``number_state_date`` key used to match sources."""
    state_part = (str(state).upper() if state else "N/A") or "N/A"
    date_part = _date_str(when) or "N/A"
    return f"{normalized_number}_{state_part}_{date_part}"


def _to_date(value: object) -> Optional[date]:
    """Coerce a date / ISO date string into a ``date``; None on missing/invalid."""
    if value is None or value == "" or value == "N/A":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _date_str(value: object) -> Optional[str]:
    d = _to_date(value)
    return d.isoformat() if d else None


def dates_within_range(a: object, b: object, days: int = 3) -> bool:
    """True when two date-like values are within +/- ``days`` of each other."""
    da, db = _to_date(a), _to_date(b)
    if da is None or db is None:
        return False
    return abs((da - db).days) <= days


def similarity(a: object, b: object) -> float:
    """Return a 0-100 similarity ratio between two strings (difflib-based)."""
    sa, sb = str(a or "").lower().strip(), str(b or "").lower().strip()
    if not sa and not sb:
        return 0.0
    return round(SequenceMatcher(None, sa, sb).ratio() * 100.0, 2)


def match_confidence(legiscan_bill: Bill, openstates_bill: Bill) -> float:
    """Average title + summary similarity; skips fields empty on both sides."""
    field_pairs = (
        (legiscan_bill.title, openstates_bill.title),
        (legiscan_bill.summary, openstates_bill.summary),
    )
    scores = [
        similarity(a, b)
        for a, b in field_pairs
        if str(a or "").strip() or str(b or "").strip()
    ]
    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 2)


def is_same_bill(
    legiscan_bill: Bill,
    openstates_bill: Bill,
    *,
    days: int = 3,
    min_confidence: float = 60.0,
) -> bool:
    """Decide whether a LegiScan and an OpenStates record are the same bill.

    Requires same normalized number, same state, and either a close version_date
    or a high enough title/summary similarity (so a genuine match still lands when
    the two sources disagree on the exact action date).
    """
    if (legiscan_bill.state or "").upper() != (openstates_bill.state or "").upper():
        return False

    legi_num = normalize_legiscan_bill_number(legiscan_bill.bill_number)
    open_num = normalize_openstates_identifier(openstates_bill.bill_number)
    if not legi_num or legi_num != open_num:
        return False

    if dates_within_range(legiscan_bill.version_date, openstates_bill.version_date, days):
        return True
    return match_confidence(legiscan_bill, openstates_bill) >= min_confidence
