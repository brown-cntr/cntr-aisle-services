"""
Bills table Supabase access: read existing bills, store new bills, update existing.
"""
import logging
from datetime import datetime
from typing import List

from shared.models.bill import Bill

logger = logging.getLogger(__name__)


class BillsRepository:
    """Single place for all bills-table Supabase access."""

    # Chunk size for bulk legiscan_id lookups 
    LEGISCAN_ID_CHUNK_SIZE = 500

    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def get_last_run_timestamp(self):
        """
        Get the timestamp of the last successful ingestion run.
        Uses the most recent updated_at from bills table (when we last processed a bill).

        Returns:
            Last run datetime or None if no previous runs
        """
        try:
            result = (
                self.supabase.table("bills")
                .select("updated_at")
                .order("updated_at", desc=True)
                .limit(1)
                .execute()
            )

            if result.data and result.data[0].get("updated_at"):
                last_run = datetime.fromisoformat(
                    result.data[0]["updated_at"].replace("Z", "+00:00")
                )
                logger.info(f"Last ingestion run: {last_run}")
                return last_run

            logger.info("No previous ingestion found, will process all bills")
            return None

        except Exception as e:
            logger.warning(
                f"Error getting last run timestamp: {e}, will process all bills"
            )
            return None

    def get_existing_legiscan_ids_in_list(self, bill_ids: List[int]) -> set[int]:
        """
        Bulk check: return which of the given LegiScan bill_ids already exist
        in the database (by legiscan_id).

        Args:
            bill_ids: LegiScan bill IDs from search results

        Returns:
            Set of bill_ids that are already in the bills table
        """
        if not bill_ids:
            return set()
        bill_ids = list(dict.fromkeys(bill_ids))  # dedupe
        existing = set()
        for i in range(0, len(bill_ids), self.LEGISCAN_ID_CHUNK_SIZE):
            chunk = bill_ids[i : i + self.LEGISCAN_ID_CHUNK_SIZE]
            try:
                result = (
                    self.supabase.table("bills")
                    .select("legiscan_id")
                    .in_("legiscan_id", chunk)
                    .execute()
                )
                for row in result.data or []:
                    if row.get("legiscan_id") is not None:
                        existing.add(int(row["legiscan_id"]))
            except Exception as e:
                logger.warning(
                    f"Error bulk-checking legiscan_ids for chunk: {e}"
                )
        logger.info(
            f"Bulk check: {len(existing)} of {len(bill_ids)} search-result bills already in database"
        )
        return existing

    def get_existing_bills_map(self) -> dict[str, dict]:
        """
        Get mapping of external_id to bill data (version_date, updated_at) for comparison.

        Returns:
            Dictionary mapping external_id to bill metadata
        """
        try:
            result = (
                self.supabase.table("bills")
                .select("external_id, version_date, updated_at")
                .execute()
            )

            bills_map = {}
            for row in result.data:
                if row.get("external_id"):
                    bills_map[row["external_id"]] = {
                        "version_date": row.get("version_date"),
                        "updated_at": row.get("updated_at"),
                    }

            logger.info(f"Retrieved metadata for {len(bills_map)} existing bills")
            return bills_map

        except Exception as e:
            logger.warning(f"Error getting existing bills map: {e}")
            return {}

    def get_existing_bills_by_state_number(self) -> dict[str, list[dict]]:
        """
        Get existing bills grouped by state and bill_number for efficient filtering.

        Returns:
            Dictionary mapping "{state} {bill_number}" to list of bills with that state+number
            (a bill can have multiple entries if it has different version_dates)
        """
        bills_by_key: dict[str, list[dict]] = {}
        cursor = ""
        page_size = 1000
        try:
            while True:
                query = (
                    self.supabase.table("bills")
                    .select("state, bill_number, external_id, version_date")
                    .order("external_id")
                    .limit(page_size)
                )
                if cursor:
                    query = query.gt("external_id", cursor)
                result = query.execute()
                page = result.data or []
                for row in page:
                    state = row.get("state", "")
                    bill_number = row.get("bill_number", "")
                    if state and bill_number:
                        key = f"{state} {bill_number}"
                        if key not in bills_by_key:
                            bills_by_key[key] = []
                        bills_by_key[key].append({
                            "external_id": row.get("external_id"),
                            "version_date": row.get("version_date"),
                        })
                if len(page) < page_size:
                    break
                cursor = page[-1]["external_id"]
            logger.info(
                f"Retrieved {len(bills_by_key)} unique state+bill_number combinations"
            )
            return bills_by_key
        except Exception as e:
            logger.warning(
                f"Error getting existing bills by state+number: {e}"
            )
            return {}

    @staticmethod
    def _bill_to_row(bill: Bill) -> dict:
        """Build a database row dict from a Bill model (exclude id, timestamps, full_text)."""
        row = bill.model_dump(
            exclude={"id", "created_at", "updated_at", "full_text"},
            exclude_none=True,
        )
        if not row.get("url") and bill.legiscan_url:
            row["url"] = bill.legiscan_url
        if bill.version_date:
            row["version_date"] = bill.version_date.isoformat()
        return row

    def get_legiscan_ids_missing_session(self) -> List[int]:
        """Return legiscan_ids of bills that are missing legiscan_session_id.

        Uses cursor-based pagination on legiscan_id to reliably fetch all rows
        regardless of server-side max_rows limits.
        """
        ids: List[int] = []
        cursor = 0
        page_size = 1000
        try:
            while True:
                result = (
                    self.supabase.table("bills")
                    .select("legiscan_id")
                    .is_("legiscan_session_id", "null")
                    .not_.is_("legiscan_id", "null")
                    .gt("legiscan_id", cursor)
                    .order("legiscan_id")
                    .limit(page_size)
                    .execute()
                )
                page = result.data or []
                for row in page:
                    if row.get("legiscan_id") is not None:
                        ids.append(int(row["legiscan_id"]))
                if len(page) < page_size:
                    break
                cursor = ids[-1]
            logger.info(f"Found {len(ids)} bills missing legiscan_session_id")
            return ids
        except Exception as e:
            logger.warning(f"Error fetching bills missing session ID: {e}")
            return []

    def get_distinct_session_ids(self) -> List[int]:
        """Return distinct legiscan_session_ids present in the bills table."""
        ids: set[int] = set()
        offset = 0
        page_size = 1000
        try:
            while True:
                result = (
                    self.supabase.table("bills")
                    .select("legiscan_session_id")
                    .not_.is_("legiscan_session_id", "null")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                page = result.data or []
                for row in page:
                    if row.get("legiscan_session_id") is not None:
                        ids.add(int(row["legiscan_session_id"]))
                if len(page) < page_size:
                    break
                offset += page_size
            logger.info(f"Found {len(ids)} distinct session IDs in database")
            return list(ids)
        except Exception as e:
            logger.warning(f"Error fetching distinct session IDs: {e}")
            return []

    def get_change_hashes_for_session(self, session_id: int) -> dict[int, str]:
        """Return {legiscan_id: change_hash} for all bills in the given session.

        Bills with a NULL change_hash are included with an empty string so they
        are always treated as changed. Paginates to handle large sessions.
        """
        hashes: dict[int, str] = {}
        offset = 0
        page_size = 1000
        try:
            while True:
                result = (
                    self.supabase.table("bills")
                    .select("legiscan_id, change_hash")
                    .eq("legiscan_session_id", session_id)
                    .not_.is_("legiscan_id", "null")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                page = result.data or []
                for row in page:
                    if row.get("legiscan_id") is not None:
                        hashes[int(row["legiscan_id"])] = row.get("change_hash") or ""
                if len(page) < page_size:
                    break
                offset += page_size
            return hashes
        except Exception as e:
            logger.warning(
                f"Error fetching change hashes for session {session_id}: {e}"
            )
            return {}

    def update_bill_by_legiscan_id(self, legiscan_id: int, bill: Bill) -> bool:
        """Full update of a bill row matched by legiscan_id."""
        try:
            bill_dict = self._bill_to_row(bill)
            bill_dict["updated_at"] = datetime.now().isoformat()
            self.supabase.table("bills").update(bill_dict).eq(
                "legiscan_id", legiscan_id
            ).execute()
            logger.debug(f"Updated bill legiscan_id={legiscan_id}")
            return True
        except Exception as e:
            logger.error(
                f"Error updating bill legiscan_id={legiscan_id}: {e}", exc_info=True
            )
            return False

    def _handle_existing_bill(
        self, existing_bill: dict, bill: Bill, now: str
    ) -> None:
        """
        Update metadata on an existing bill row matched by legiscan_id.
        """
        updates = {}

        if bill.bill_status is not None:
            updates["bill_status"] = bill.bill_status

        if bill.change_hash is not None:
            updates["change_hash"] = bill.change_hash

        if bill.legiscan_session_id is not None:
            updates["legiscan_session_id"] = bill.legiscan_session_id

        # Keep external_id current
        if bill.external_id and bill.external_id != existing_bill.get("external_id"):
            updates["external_id"] = bill.external_id

        if not updates:
            return

        updates["updated_at"] = now
        try:
            self.supabase.table("bills").update(updates).eq(
                "id", existing_bill["id"]
            ).execute()
            logger.debug(
                f"Updated existing bill legiscan_id={bill.legiscan_id}: {list(updates.keys())}"
            )
        except Exception as e:
            logger.warning(
                f"Could not update existing bill legiscan_id={bill.legiscan_id}: {e}"
            )

    def store_bills(self, bills: List[Bill]) -> int:
        """
        Store bills in Supabase. Inserts new bills and skips any already
        present (matched by legiscan_id). Bills from a different session
        sharing the same bill number are always inserted as new rows because
        they carry a distinct legiscan_id.

        Args:
            bills: List of Bill model instances

        Returns:
            Number of bills successfully inserted (new only)
        """
        count = 0
        new_count = 0
        skipped_count = 0
        now = datetime.now().isoformat()

        for i, bill in enumerate(bills, 1):
            try:
                logger.debug(
                    f"Storing bill {bill.external_id} ({i}/{len(bills)})..."
                )

                # Look up by legiscan_id and falls back to external_id if needed
                existing = None
                if bill.legiscan_id is not None:
                    result = (
                        self.supabase.table("bills")
                        .select("id, external_id, legiscan_id")
                        .eq("legiscan_id", bill.legiscan_id)
                        .execute()
                    )
                    if result.data:
                        existing = result

                if existing is None and bill.external_id:
                    result = (
                        self.supabase.table("bills")
                        .select("id, external_id, legiscan_id")
                        .eq("external_id", bill.external_id)
                        .execute()
                    )
                    if result.data:
                        existing = result

                bill_dict = self._bill_to_row(bill)

                if existing and existing.data:
                    self._handle_existing_bill(existing.data[0], bill, now)
                    logger.debug(
                        f"Skipped bill {bill.external_id} (already in database)"
                    )
                    skipped_count += 1
                else:
                    bill_dict["created_at"] = now
                    bill_dict["updated_at"] = now
                    try:
                        result = (
                            self.supabase.table("bills")
                            .insert(bill_dict)
                            .execute()
                        )
                        result_data = getattr(result, "data", []) or []
                        if len(result_data) == 0:
                            logger.warning(
                                f"Insert for {bill.external_id} returned no data - may have failed"
                            )
                        logger.debug(
                            f"Inserted new bill {bill.external_id}, "
                            f"result count={len(result_data)}"
                        )
                        new_count += 1
                        count += 1
                    except Exception as insert_error:
                        logger.error(
                            f"Error inserting {bill.external_id}: {insert_error}",
                            exc_info=True,
                        )

            except Exception as e:
                logger.error(
                    f"Error storing bill {bill.external_id}: {e}",
                    exc_info=True,
                )
                continue

        logger.info(
            f"Storage complete: {new_count} new, {skipped_count} skipped (already in DB), {count} total inserted"
        )
        if count == 0 and len(bills) > 0:
            logger.warning(
                f"WARNING: Processed {len(bills)} bills but count=0. All were skipped!"
            )
        return count

    def update_existing_bill(self, bill_id: str, bill: Bill) -> bool:
        """
        Update an existing bill in Supabase

        Args:
            bill_id: Supabase bill UUID
            bill: Updated Bill model instance

        Returns:
            True if successful, False otherwise
        """
        try:
            bill_dict = bill.model_dump(
                exclude={"id", "created_at"}, exclude_none=True
            )
            bill_dict["updated_at"] = datetime.now().isoformat()

            result = (
                self.supabase.table("bills")
                .update(bill_dict)
                .eq("id", bill_id)
                .execute()
            )

            logger.info(f"Updated bill {bill_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating bill {bill_id}: {e}")
            return False