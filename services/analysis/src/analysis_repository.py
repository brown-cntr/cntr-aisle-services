"""
Supabase access for the analysis service: read bills that have full text, and
persist computed bill-to-bill similarities.
"""
import logging
from datetime import datetime
from typing import List, Optional

from shared.models.bill_similarity import BillSimilarity

from .similarity import BillText

logger = logging.getLogger(__name__)


class AnalysisRepository:
    """Reads bill text and writes similarity relationships."""

    PAGE_SIZE = 1000

    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def get_bills_with_text(self, limit: Optional[int] = None) -> List[BillText]:
        """Fetch bills that have non-empty ``full_text`` as lightweight BillText views.

        Paginates on ``external_id`` so it reliably returns all rows regardless of
        server-side row caps.
        """
        bills: List[BillText] = []
        cursor = ""
        try:
            while True:
                page_size = self.PAGE_SIZE
                if limit is not None:
                    remaining = limit - len(bills)
                    if remaining <= 0:
                        break
                    page_size = min(page_size, remaining)

                query = (
                    self.supabase.table("bills")
                    .select("external_id, full_text")
                    .not_.is_("full_text", "null")
                    .order("external_id")
                    .limit(page_size)
                )
                if cursor:
                    query = query.gt("external_id", cursor)
                result = query.execute()
                page = result.data or []
                for row in page:
                    ext = row.get("external_id")
                    if ext and row.get("full_text"):
                        bills.append(BillText(external_id=ext, full_text=row["full_text"]))
                if len(page) < page_size:
                    break
                cursor = page[-1]["external_id"]
            logger.info(f"Loaded {len(bills)} bills with full_text")
            return bills
        except Exception as e:
            logger.warning(f"Error loading bills with text: {e}")
            return bills

    def store_similarities(self, similarities: List[BillSimilarity]) -> int:
        """Upsert similarity rows into ``bill_similarities``; return count written."""
        if not similarities:
            return 0
        now = datetime.now().isoformat()
        rows = []
        for sim in similarities:
            row = sim.model_dump(exclude={"id", "created_at"}, exclude_none=True)
            row["created_at"] = now
            rows.append(row)
        try:
            self.supabase.table("bill_similarities").upsert(
                rows, on_conflict="source_bill_id,target_bill_id"
            ).execute()
            logger.info(f"Stored {len(rows)} similarity relationships")
            return len(rows)
        except Exception as e:
            logger.error(f"Error storing similarities: {e}", exc_info=True)
            return 0
