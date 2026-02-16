"""
Parse LegiScan API bill data into our Bill model.
"""
import re
import logging
from typing import Any, Dict

from datetime import datetime
from shared.models.bill import Bill, BillBody

logger = logging.getLogger(__name__)


def parse_bill_data(legiscan_bill: Dict[str, Any]) -> Bill:
    """
    Parse LegiScan bill data into our Bill model.

    Args:
        legiscan_bill: Raw bill data from LegiScan API

    Returns:
        Bill model instance
    """
    # Extract body/chamber type
    chamber = legiscan_bill.get("chamber", "") or legiscan_bill.get("body", "")
    body = _map_chamber_to_body(chamber, legiscan_bill.get("bill_number", ""))

    # Extract year from session
    year = legiscan_bill.get("year")
    if not year and legiscan_bill.get("session"):
        session_title = legiscan_bill.get("session", {}).get("session_title", "")
        year_match = re.search(r"(\d{4})", session_title)
        if year_match:
            year = int(year_match.group(1))

    if not year:
        year = datetime.now().year

    # Parse status date (use status_date as version_date)
    version_date = None
    status_date_str = legiscan_bill.get("status_date")
    if status_date_str:
        try:
            version_date = datetime.strptime(status_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    first_action_date = None
    if legiscan_bill.get("history") and len(legiscan_bill["history"]) > 0:
        first_action = legiscan_bill["history"][0]
        if first_action.get("date"):
            try:
                first_action_date = datetime.strptime(
                    first_action["date"], "%Y-%m-%d"
                ).date()
            except (ValueError, TypeError):
                pass

    if not version_date and first_action_date:
        version_date = first_action_date

    state = legiscan_bill.get("state", "").upper()
    bill_id = legiscan_bill.get("bill_id")
    legiscan_url = legiscan_bill.get("url") or (
        f"https://legiscan.com/{state}/bill/{bill_id}" if bill_id else None
    )
    url = legiscan_bill.get("state_link") or legiscan_url

    bill_number_raw = legiscan_bill.get("bill_number", "")
    version_date_str = version_date.isoformat() if version_date else ""

    if state and bill_number_raw and version_date_str:
        external_id = f"{state} {bill_number_raw} {version_date_str}"
    else:
        logger.warning(
            f"Cannot construct external_id for bill {bill_id}: "
            f"state={state}, bill_number={bill_number_raw}, version_date={version_date_str}. "
            f"Using bill_id as fallback."
        )
        external_id = str(bill_id) if bill_id else ""

    return Bill(
        external_id=external_id,
        title=legiscan_bill.get("title", ""),
        state=state,
        year=year,
        bill_number=bill_number_raw,
        body=body,
        summary=legiscan_bill.get("description"),
        url=url,
        legiscan_url=legiscan_url,
        legiscan_id=bill_id,
        version_date=version_date,
    )

def _map_chamber_to_body(chamber: str, bill_number: str) -> BillBody:
    """
    Map LegiScan chamber/body field to our BillBody enum.
    """
    chamber_upper = chamber.upper() if chamber else ""

    if "HOUSE" in chamber_upper or "ASSEMBLY" in chamber_upper:
        return BillBody.HOUSE if "HOUSE" in chamber_upper else BillBody.ASSEMBLY
    if "SENATE" in chamber_upper:
        return BillBody.SENATE

    bill_upper = bill_number.upper() if bill_number else ""
    if bill_upper.startswith("H") or bill_upper.startswith("HB") or bill_upper.startswith("HR"):
        return BillBody.HOUSE
    if bill_upper.startswith("S") or bill_upper.startswith("SB") or bill_upper.startswith("SR"):
        return BillBody.SENATE
    if bill_upper.startswith("A") or bill_upper.startswith("AB"):
        return BillBody.ASSEMBLY

    logger.warning(
        f"Could not determine body for chamber={chamber}, bill_number={bill_number}, defaulting to HOUSE"
    )
    return BillBody.HOUSE
