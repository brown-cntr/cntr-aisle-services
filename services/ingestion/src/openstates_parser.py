"""
Parse an OpenStates v3 bill object into the shared Bill model.

The OpenStates-side counterpart of parser.parse_bill_data, so an OpenStates
record can be reconciled against LegiScan bills and stored in the same table.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

from shared.models.bill import Bill, BillSource

from .parser import _map_chamber_to_body

logger = logging.getLogger(__name__)

# OpenStates chamber classification -> hint used with the shared chamber mapper.
_ORG_CLASSIFICATION_HINT = {
    "lower": "House",
    "upper": "Senate",
    "legislature": "",
}


def _extract_state(jurisdiction: Any) -> str:
    """Pull a 2-letter state code from an OpenStates jurisdiction object.

    Jurisdiction ids look like ``ocd-jurisdiction/country:us/state:ca/government``.
    Falls back to the jurisdiction ``name`` when the id is not in that form.
    """
    if isinstance(jurisdiction, dict):
        juris_id = jurisdiction.get("id", "") or ""
        match = re.search(r"state:([a-z]{2})\b", juris_id)
        if match:
            return match.group(1).upper()
        name = jurisdiction.get("name", "") or ""
        return name.upper()[:2] if name else ""
    if isinstance(jurisdiction, str):
        match = re.search(r"state:([a-z]{2})\b", jurisdiction)
        if match:
            return match.group(1).upper()
    return ""


def _extract_abstract(bill: Dict[str, Any]) -> Optional[str]:
    """Return the first non-empty abstract text, if any."""
    abstracts = bill.get("abstracts") or []
    for entry in abstracts:
        if isinstance(entry, dict):
            text = entry.get("abstract") or entry.get("note")
            if text:
                return str(text)
    return None


def _extract_year(bill: Dict[str, Any], version_date: Optional[datetime]) -> int:
    """Determine the bill year from the session string, else the action date."""
    session = bill.get("session") or ""
    match = re.search(r"(\d{4})", str(session))
    if match:
        return int(match.group(1))
    if version_date is not None:
        return version_date.year
    return datetime.now().year


def _first_source_url(bill: Dict[str, Any]) -> Optional[str]:
    sources = bill.get("sources") or []
    for entry in sources:
        if isinstance(entry, dict) and entry.get("url"):
            return str(entry["url"])
    return None


def parse_openstates_bill(os_bill: Dict[str, Any]) -> Bill:
    """Convert an OpenStates v3 bill dict into a :class:`Bill` (source = openstates)."""
    identifier = str(os_bill.get("identifier", "") or "")
    state = _extract_state(os_bill.get("jurisdiction"))

    version_date = None
    first_action = os_bill.get("first_action_date") or os_bill.get("latest_action_date")
    if first_action:
        try:
            version_date = datetime.strptime(str(first_action)[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            version_date = None

    year = _extract_year(os_bill, version_date)

    from_org = os_bill.get("from_organization") or {}
    chamber_hint = ""
    if isinstance(from_org, dict):
        chamber_hint = _ORG_CLASSIFICATION_HINT.get(
            (from_org.get("classification") or "").lower(), ""
        )
    body = _map_chamber_to_body(chamber_hint, identifier)

    openstates_url = os_bill.get("openstates_url") or None
    source_url = _first_source_url(os_bill) or openstates_url

    version_date_str = version_date.date().isoformat() if version_date else ""
    openstates_id = os_bill.get("id")
    if state and identifier and version_date_str:
        external_id = f"{state} {identifier} {version_date_str}"
    else:
        external_id = str(openstates_id or identifier or "")
        if not external_id:
            logger.warning("OpenStates bill missing id/identifier; external_id empty")

    return Bill(
        external_id=external_id,
        title=str(os_bill.get("title", "") or ""),
        state=state,
        year=year,
        bill_number=identifier,
        body=body,
        summary=_extract_abstract(os_bill),
        url=source_url,
        openstates_id=str(openstates_id) if openstates_id else None,
        openstates_url=openstates_url,
        version_date=version_date.date() if version_date else None,
        source=BillSource.OPENSTATES.value,
    )
