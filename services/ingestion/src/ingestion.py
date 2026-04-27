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

    def backfill_session_data(self, dry_run: bool = False) -> int:
        """
        One-time backfill: populate change_hash and legiscan_session_id for
        existing bills that are missing those fields.

        Fetches via getBill matched on legiscan_id (not external_id), so it
        works correctly even if version_date has changed since the bill was inserted.

        Returns:
            Number of bills updated (or would be updated in dry run)
        """
        if dry_run:
            logger.info("DRY RUN: No database writes will be performed")
        logger.info("Starting session data backfill...")

        legiscan_ids = self.bills_repository.get_legiscan_ids_missing_session()
        if not legiscan_ids:
            logger.info("All bills already have legiscan_session_id — nothing to backfill")
            return 0

        logger.info(f"Backfilling {len(legiscan_ids)} bill(s)...")

        from .parser import parse_bill_data
        updated = 0

        for bill_id in legiscan_ids:
            try:
                bill_data = self.legiscan_client.get_bill(bill_id)
                if not bill_data:
                    logger.warning(f"No data for bill {bill_id}, skipping")
                    continue
                bill = parse_bill_data(bill_data)
                if dry_run:
                    logger.info(
                        f"  Would backfill: {bill.state} {bill.bill_number} "
                        f"(legiscan_id={bill_id}, session={bill.legiscan_session_id})"
                    )
                else:
                    self.bills_repository.update_bill_by_legiscan_id(bill_id, bill)
                updated += 1
            except Exception as e:
                logger.error(f"Error backfilling bill {bill_id}: {e}", exc_info=True)
                continue

        logger.info(f"Backfill complete: {updated} bill(s) updated")
        return updated

    def sync_bills(self, dry_run: bool = False) -> int:
        """
        Sync existing bills using getMasterListRaw change_hash comparison.

        For each session that has bills in the database, fetches the current
        change_hash for every bill and updates only those whose hash has changed.

        Returns:
            Number of bills updated (or would be updated in dry run)
        """
        if dry_run:
            logger.info("DRY RUN: No database writes will be performed")
        logger.info("Starting bill sync...")

        session_ids = self.bills_repository.get_distinct_session_ids()
        if not session_ids:
            logger.warning("No sessions found in database; nothing to sync")
            return 0

        logger.info(f"Checking {len(session_ids)} session(s) for changes")

        total_updated = 0

        for session_id in session_ids:
            try:
                api_hashes = self.legiscan_client.get_master_list_raw(session_id)
                db_hashes = self.bills_repository.get_change_hashes_for_session(
                    session_id
                )

                # Only consider bill_ids we already track in the DB
                changed_ids = [
                    bill_id
                    for bill_id, api_hash in api_hashes.items()
                    if bill_id in db_hashes and db_hashes[bill_id] != api_hash
                ]

                logger.info(
                    f"Session {session_id}: {len(db_hashes)} bills tracked, "
                    f"{len(changed_ids)} changed"
                )

                for bill_id in changed_ids:
                    try:
                        bill_data = self.legiscan_client.get_bill(bill_id)
                        if not bill_data:
                            logger.warning(
                                f"No data returned for bill {bill_id}, skipping"
                            )
                            continue

                        from .parser import parse_bill_data
                        bill = parse_bill_data(bill_data)

                        if dry_run:
                            logger.info(
                                f"  Would update: {bill.state} {bill.bill_number} "
                                f"(legiscan_id={bill_id}) — {bill.bill_status}"
                            )
                        else:
                            self.bills_repository.update_bill_by_legiscan_id(
                                bill_id, bill
                            )

                        total_updated += 1

                    except Exception as e:
                        logger.error(
                            f"Error syncing bill {bill_id}: {e}", exc_info=True
                        )
                        continue

            except Exception as e:
                logger.error(
                    f"Error syncing session {session_id}: {e}", exc_info=True
                )
                continue

        logger.info(f"Sync complete: {total_updated} bill(s) updated")
        return total_updated
