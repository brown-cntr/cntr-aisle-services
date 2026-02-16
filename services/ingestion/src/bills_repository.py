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
        try:
            result = (
                self.supabase.table("bills")
                .select("state, bill_number, external_id, version_date")
                .execute()
            )

            bills_by_key = {}
            for row in result.data:
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

    def _handle_existing_bill(
        self, existing_bill: dict, bill: Bill, now: str
    ) -> None:
        """Backfill legiscan_id on existing row if missing."""
        existing_legiscan_id = existing_bill.get("legiscan_id")
        if existing_legiscan_id is not None or bill.legiscan_id is None:
            return
        backfill = {"legiscan_id": bill.legiscan_id, "updated_at": now}
        if bill.legiscan_url:
            backfill["legiscan_url"] = bill.legiscan_url
        try:
            self.supabase.table("bills").update(backfill).eq(
                "id", existing_bill["id"]
            ).execute()
            logger.debug(
                f"Backfilled legiscan_id for {bill.external_id} (was missing)"
            )
        except Exception as e:
            logger.warning(
                f"Could not backfill legiscan_id for {bill.external_id}: {e}"
            )

    @staticmethod
    def _is_duplicate_key_error(exc: Exception) -> bool:
        """True if the exception is a Postgres unique violation for bills_state_year_bill_number_body_key."""
        s = str(exc) + repr(exc)
        if "bills_state_year_bill_number_body_key" not in s:
            return False
        return "23505" in s or "duplicate key" in s.lower()

    def store_bills(self, bills: List[Bill]) -> int:
        """
        Store bills in Supabase. Only inserts new bills; skips any already
        present (by external_id or by state/year/bill_number/body).

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

                existing = (
                    self.supabase.table("bills")
                    .select("id, external_id, version_date, legiscan_id")
                    .eq("external_id", bill.external_id)
                    .execute()
                )

                bill_dict = self._bill_to_row(bill)

                if existing.data:
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
                        if self._is_duplicate_key_error(insert_error):
                            logger.info(
                                f"Skipped bill {bill.external_id} "
                                f"(duplicate key, already in database)"
                            )
                            skipped_count += 1
                        else:
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