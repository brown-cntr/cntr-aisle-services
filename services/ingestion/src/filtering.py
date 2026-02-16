"""
Filter which bills to process
"""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def filter_bills_for_processing(
    search_results: List[Dict[str, Any]],
    existing_bills_by_state_number: dict[str, list[dict]],
    check_existing: bool = False,
    raw_search_results: bool = True,

) -> List[Dict[str, Any]]:
    """
    Filter search results to only include bills that need processing.

    Args:
        search_results: List of search result dictionaries from LegiScan
        existing_bills_by_state_number: Dict mapping "{state} {bill_number}" to list of existing bills
        check_existing: If True, process all bills to check for updates (default: False)
        raw_search_results: If True, the search results are from getSearchRaw (no state/bill_number)
    Returns:
        Filtered list of search results to process
    """
    if raw_search_results:
        return search_results
        
    filtered = []
    new_count = 0
    potentially_updated_count = 0

    for result in search_results:
        state = result.get("state", "").upper()
        bill_number = result.get("bill_number", "")

        if not state or not bill_number:
            # Skip if we can't identify the bill (getSearch results)
            continue

        key = f"{state} {bill_number}"

        if key not in existing_bills_by_state_number:
            filtered.append(result)
            new_count += 1
        else:
            filtered.append(result)
            potentially_updated_count += 1

    logger.info(
        f"Filtered search results: {new_count} new state+bill_number combinations, "
        f"{potentially_updated_count} existing state+bill_number to check for new versions, "
        f"{len(filtered)} total to process"
    )

    return filtered
