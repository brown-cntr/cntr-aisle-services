"""
Orchestration for the analysis service: load bills with text, compute pairwise
similarities, and persist the relationships.
"""
import logging
from typing import Optional

from shared.database.supabase_client import get_supabase_client

from .analysis_repository import AnalysisRepository
from .similarity import (
    DEFAULT_RELATED_THRESHOLD,
    DEFAULT_SHINGLE_K,
    compare_all_pairs,
)

logger = logging.getLogger(__name__)


class AnalysisService:
    """Compute and store bill-to-bill similarity relationships."""

    def __init__(self, repository: Optional[AnalysisRepository] = None):
        self.repository = repository or AnalysisRepository(get_supabase_client())

    def run_comparison(
        self,
        min_score: float = DEFAULT_RELATED_THRESHOLD,
        k: int = DEFAULT_SHINGLE_K,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> int:
        """Compare all bills with text pairwise and store matches >= ``min_score``.

        Returns the number of similarity relationships stored (or that would be
        stored, in dry-run).
        """
        if dry_run:
            logger.info("DRY RUN: no similarity rows will be written")

        bills = self.repository.get_bills_with_text(limit=limit)
        if len(bills) < 2:
            logger.info("Fewer than 2 bills with full_text; nothing to compare")
            return 0

        logger.info(f"Comparing {len(bills)} bills pairwise (min_score={min_score})")
        similarities = compare_all_pairs(bills, min_score=min_score, k=k)
        logger.info(f"Found {len(similarities)} similarity relationships")

        if dry_run:
            for sim in similarities[:5]:
                logger.info(
                    f"  {sim.source_bill_id} <-> {sim.target_bill_id}: "
                    f"{sim.score} ({sim.classification})"
                )
            return len(similarities)

        return self.repository.store_similarities(similarities)
