"""CLI entrypoint for ingestion service"""
import argparse
import logging
import re
import sys
from datetime import date, datetime
from typing import Optional

from shared.database.supabase_client import get_supabase_client

from .bills_repository import BillsRepository
from .ingestion import IngestionService
from .legiscan_client import LegiScanClient
from .parser import parse_bill_data

logger = logging.getLogger(__name__)


def _extract_legiscan_id_from_url(url: str) -> Optional[int]:
    """
    Extract LegiScan bill_id from a LegiScan bill URL.

    Expected formats, for example:
      - https://legiscan.com/CA/bill/123456
      - https://legiscan.com/CA/bill/123456/2023
    """
    match = re.search(r"legiscan\.com/[^/]+/bill/(\d+)", url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _parse_legiscan_bill_url(url: str) -> Optional[tuple[str, str, int]]:
    """
    Parse a LegiScan bill URL into (state, bill_label, year).

    Expected formats, for example:
      - https://legiscan.com/IL/bill/SB3890/2025
    """
    match = re.search(r"legiscan\.com/([^/]+)/bill/([^/]+)/([0-9]{4})", url)
    if not match:
        return None
    state, bill_label, year_str = match.group(1), match.group(2), match.group(3)
    try:
        year = int(year_str)
    except ValueError:
        return None
    return state.upper(), bill_label, year


def _resolve_bill_id_from_url(url: str) -> int:
    """
    Resolve a non-numeric LegiScan bill URL into a numeric bill_id using getSearch.

    Uses the state, bill label, and year parsed from the URL to query LegiScan and
    then selects the unique matching result.
    """
    parsed = _parse_legiscan_bill_url(url)
    if not parsed:
        raise ValueError(
            "Could not parse LegiScan bill URL into (state, bill, year) components"
        )

    state, bill_label, year = parsed

    client = LegiScanClient()
    data = client._make_request("getSearch", state=state, query=bill_label, year=year)  # type: ignore[attr-defined]

    search_result = data.get("searchresult") or {}

    # getSearch response uses numeric keys plus a 'summary' object
    candidates: list[dict] = []
    for key, value in search_result.items():
        if key == "summary":
            continue
        if not isinstance(value, dict):
            continue
        result_state = (value.get("state") or "").upper()
        result_bill_number = value.get("bill_number") or ""
        if result_state != state or result_bill_number.upper() != bill_label.upper():
            continue
        candidates.append(value)

    if not candidates:
        raise ValueError(
            f"No LegiScan search results matched {state} {bill_label} {year}"
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple LegiScan search results matched {state} {bill_label} {year}; "
            "cannot determine unique bill_id"
        )

    bill_id = candidates[0].get("bill_id")
    if bill_id is None:
        raise ValueError("Matched LegiScan search result is missing bill_id")

    try:
        return int(bill_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid bill_id returned from LegiScan search: {bill_id!r}") from exc


def _ingest_single_bill_by_legiscan_id(bill_id: int) -> int:
    """
    Fetch a single bill from LegiScan by bill_id and store it in Supabase.

    Returns the number of bills inserted (0 if already present).
    Raises RuntimeError on hard failures.
    """
    client = LegiScanClient()
    bill_data = client.get_bill(bill_id)
    if not bill_data:
        raise RuntimeError(f"No data returned for LegiScan bill {bill_id}")

    bill = parse_bill_data(bill_data)

    repository = BillsRepository(get_supabase_client())
    return repository.store_bills([bill])


def main():
    """Main entry point for ingestion service."""
    parser = argparse.ArgumentParser(
        description="Ingest AI-related bills from LegiScan"
    )
    parser.add_argument(
        "--min-relevance",
        type=int,
        default=0,
        help="Minimum relevance score (0-100, default: 0)",
    )
    parser.add_argument(
        "--state",
        type=str,
        default="ALL",
        help="State code (e.g., CA, NY) or 'ALL' for all states (default: ALL)",
    )
    parser.add_argument(
        "--use-raw",
        action="store_true",
        default=True,
        help="Use getSearchRaw for bulk results (default: True)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Run full ingestion (process all bills, including existing ones, default: False)",
    )
    parser.add_argument(
        "--check-existing",
        action="store_true",
        default=False,
        help="Also check existing bills for updates (slower but catches all changes, default: False)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Search and fetch bills but do not insert or update in the database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only process N bills (for testing)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        metavar="DATE",
        help="Only store bills with version_date on or after this date (ISO date or datetime, e.g. 2026-01-27 or 2026-01-27T12:00:00+00:00)",
    )

    parser.add_argument(
        "--sync",
        action="store_true",
        default=False,
        help="Sync existing bills using change_hash comparison via getMasterListRaw",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        default=False,
        help="One-time backfill: populate change_hash and legiscan_session_id for existing bills",
    )

    single_group = parser.add_mutually_exclusive_group()
    single_group.add_argument(
        "--legiscan-url",
        type=str,
        default=None,
        help=(
            "Ingest a single bill by its LegiScan URL "
            "(e.g. https://legiscan.com/CA/bill/123456)"
        ),
    )
    single_group.add_argument(
        "--legiscan-id",
        type=int,
        default=None,
        help="Ingest a single bill by its LegiScan numeric bill_id.",
    )

    args = parser.parse_args()

    # Parse --since to date if provided
    since_date = None
    if args.since:
        since_str = args.since.strip().replace("Z", "+00:00")
        try:
            since_date = date.fromisoformat(since_str)
        except ValueError:
            since_date = datetime.fromisoformat(since_str).date()

    # Setup logging 
    from shared.utils.config import get_settings

    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        # Single-bill ingestion path: either --legiscan-url or --legiscan-id
        if args.legiscan_url or args.legiscan_id is not None:
            if args.dry_run:
                logger.warning(
                    "--dry-run has no effect when ingesting a single bill; "
                    "proceeding with real insert."
                )

            if args.legiscan_id is not None:
                bill_id = args.legiscan_id
            else:
                bill_id = _extract_legiscan_id_from_url(args.legiscan_url)
                if bill_id is None:
                    logger.info(
                        "Could not extract numeric bill_id from URL; "
                        "resolving via LegiScan search"
                    )
                    try:
                        bill_id = _resolve_bill_id_from_url(args.legiscan_url)
                    except Exception as resolve_error:
                        logger.error(
                            "Failed to resolve LegiScan bill_id from URL via search: "
                            f"{resolve_error}"
                        )
                        sys.exit(1)

            logger.info(
                "Ingesting single bill from LegiScan",
                extra={"legiscan_bill_id": bill_id},
            )
            count = _ingest_single_bill_by_legiscan_id(bill_id)
            print(count)
            sys.exit(0)

        # Backfill path: populate change_hash / legiscan_session_id on existing bills
        if args.backfill:
            service = IngestionService()
            count = service.backfill_session_data(dry_run=args.dry_run)
            print(count)
            sys.exit(0)

        # Sync path: update existing bills via change_hash comparison
        if args.sync:
            service = IngestionService()
            count = service.sync_bills(dry_run=args.dry_run)
            print(count)
            sys.exit(0)

        # Default: full/search-based ingestion path
        service = IngestionService()
        count = service.ingest_ai_bills(
            min_relevance=args.min_relevance,
            state=args.state,
            use_raw=args.use_raw,
            incremental=not args.full,
            check_existing=args.check_existing,
            since_date=since_date,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        print(count)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)
