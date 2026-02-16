"""
Orchestration: run search -> filter -> fetch -> store for bill ingestion.
"""
import logging
from datetime import date
from typing import List, Optional

from shared.database.supabase_client import get_supabase_client
from shared.models.bill import Bill

from .bills_repository import BillsRepository
from .filtering import filter_bills_for_processing
from .legiscan_client import LegiScanClient

logger = logging.getLogger(__name__)


class IngestionService:
    """Service for ingesting bills from LegiScan into Supabase."""

    def __init__(
        self,
        legiscan_client: Optional[LegiScanClient] = None,
        bills_repository: Optional[BillsRepository] = None,
    ):
        self.legiscan_client = legiscan_client or LegiScanClient()
        self.bills_repository = bills_repository or BillsRepository(
            get_supabase_client()
        )

    @property
    def supabase(self):
        """Supabase client (e.g. for integration tests)."""
        return self.bills_repository.supabase

    def ingest_ai_bills(
        self,
        min_relevance: int = 0,
        state: str = "ALL",
        use_raw: bool = True,
        incremental: bool = True,
        check_existing: bool = False,
        since_date: Optional[date] = None,
        dry_run: bool = False,
        limit: Optional[int] = None,
    ) -> int:
        """
        Main ingestion workflow: search, fetch, and store AI-related bills

        Args:
            min_relevance: Minimum relevance score (0-100)
            state: State code or "ALL" for all states
            use_raw: Use getSearchRaw for bulk results
            incremental: If True, only process new bills or bills that may have been updated
            since_date: If set, only store bills with version_date >= this date (for time-window ingestion)
            dry_run: If True, search and fetch but do not insert or update in the database
            limit: If set, only process this many bills

        Returns:
            Number of bills successfully ingested (or would be ingested in dry run)
        """
        if dry_run:
            logger.info("DRY RUN: No database writes will be performed")
        logger.info("Starting bill ingestion process...")

        # Get existing bills for filtering
        existing_bills_by_state_number = {}
        if incremental:
            existing_bills_by_state_number = (
                self.bills_repository.get_existing_bills_by_state_number()
            )
            last_run = self.bills_repository.get_last_run_timestamp()
            if last_run:
                logger.info(f"Incremental mode: Last run was {last_run}")
            if existing_bills_by_state_number:
                total_existing = sum(
                    len(bills)
                    for bills in existing_bills_by_state_number.values()
                )
                logger.info(
                    f"Incremental mode: Found {len(existing_bills_by_state_number)} unique state+bill_number combinations "
                    f"({total_existing} total bills) in database"
                )
            else:
                logger.info(
                    "Incremental mode: No existing bills found, will process all search results"
                )

        try:
            # Step 1: Search for AI-related bills (one API call)
            logger.info("Step 1: Searching for AI-related bills...")
            search_results, summary = self.legiscan_client.search_ai_bills(
                min_relevance=min_relevance,
                state=state,
                use_raw=use_raw,
            )

            if not search_results:
                logger.warning("No bills found matching search criteria")
                return 0

            logger.info(
                f"Found {len(search_results)} bills matching search criteria"
            )

            # Step 2: Filter search results to only process bills that might be new or updated
            if incremental and existing_bills_by_state_number:
                logger.info(
                    "Step 2: Filtering search results by state+bill_number..."
                )
                search_results = filter_bills_for_processing(
                    search_results,
                    existing_bills_by_state_number,
                    check_existing=check_existing,
                )

                if not search_results:
                    logger.info(
                        "No new bills to process (all search results already in database)"
                    )
                    return 0

            if limit is not None:
                search_results = search_results[:limit]
                logger.info(
                    f"Limited to first {len(search_results)} bills"
                )

            # Bulk Supabase check: which of these search-result bill_ids are already in the DB?
            existing_legiscan_ids = None
            if incremental and not check_existing:
                search_bill_ids = []
                for r in search_results:
                    bid = r.get("bill_id")
                    if bid is not None:
                        try:
                            search_bill_ids.append(int(bid))
                        except (TypeError, ValueError):
                            pass
                if search_bill_ids:
                    existing_legiscan_ids = (
                        self.bills_repository.get_existing_legiscan_ids_in_list(
                            search_bill_ids
                        )
                    )
                    if existing_legiscan_ids:
                        logger.info(
                            f"Will skip getBill for {len(existing_legiscan_ids)} bills already in database"
                        )

            # Step 3: Fetch full metadata for each bill
            logger.info("Step 3: Fetching detailed bill metadata...")
            bills = self.legiscan_client.get_bills_from_search_results(
                search_results,
                existing_legiscan_ids=existing_legiscan_ids,
            )

            if not bills:
                logger.warning("No bills successfully fetched")
                return 0

            # Optional: filter to only bills with version_date >= since_date
            if since_date is not None:
                before_count = len(bills)
                bills = [
                    b
                    for b in bills
                    if b.version_date is not None
                    and b.version_date >= since_date
                ]
                logger.info(
                    f"Filtered to bills with version_date >= {since_date}: {len(bills)} of {before_count}"
                )
                if not bills:
                    logger.info("No bills in the requested date range")
                    return 0

            # Step 4: Store bills in Supabase (unless dry run)
            if dry_run:
                logger.info(
                    f"Step 4: DRY RUN - Would store {len(bills)} bills (no database writes)"
                )
                for b in bills[:5]:
                    logger.info(
                        f"  Would process: {b.external_id} | {b.state} {b.bill_number} | {b.title[:60]}..."
                    )
                if len(bills) > 5:
                    logger.info(f"  ... and {len(bills) - 5} more")
                ingested_count = len(bills)
            else:
                logger.info("Step 4: Storing bills in Supabase...")
                ingested_count = self.bills_repository.store_bills(bills)

            logger.info(
                f"Ingestion complete! Processed {ingested_count} bills"
            )
            return ingested_count

        except Exception as e:
            logger.error(f"Error during ingestion: {e}", exc_info=True)
            raise
