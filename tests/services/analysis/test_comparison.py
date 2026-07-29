"""Unit tests for the analysis orchestrator (AnalysisService)."""
from typing import List, Optional

from services.analysis.src.comparison import AnalysisService
from services.analysis.src.similarity import BillText
from shared.models.bill_similarity import BillSimilarity


class FakeRepo:
    """In-memory stand-in for AnalysisRepository (no Supabase)."""

    def __init__(self, bills: List[BillText]):
        self._bills = bills
        self.stored: List[BillSimilarity] = []

    def get_bills_with_text(self, limit: Optional[int] = None) -> List[BillText]:
        return self._bills if limit is None else self._bills[:limit]

    def store_similarities(self, similarities: List[BillSimilarity]) -> int:
        self.stored = list(similarities)
        return len(similarities)


def _bt(ext, text):
    return BillText(external_id=ext, full_text=text)


class TestRunComparison:
    def test_stores_matches_and_returns_count(self):
        repo = FakeRepo([
            _bt("A", "the state shall regulate artificial intelligence systems now"),
            _bt("B", "the state shall regulate artificial intelligence systems now"),
            _bt("C", "an unrelated appropriations measure for road maintenance funding"),
        ])
        service = AnalysisService(repository=repo)
        count = service.run_comparison(min_score=0.5)
        assert count == 1
        assert len(repo.stored) == 1
        assert {repo.stored[0].source_bill_id, repo.stored[0].target_bill_id} == {"A", "B"}

    def test_dry_run_does_not_store(self):
        repo = FakeRepo([
            _bt("A", "one two three four five six"),
            _bt("B", "one two three four five six"),
        ])
        service = AnalysisService(repository=repo)
        count = service.run_comparison(min_score=0.5, dry_run=True)
        assert count == 1
        assert repo.stored == []  # nothing written

    def test_fewer_than_two_bills_returns_zero(self):
        repo = FakeRepo([_bt("A", "only one bill here with text")])
        service = AnalysisService(repository=repo)
        assert service.run_comparison() == 0
        assert repo.stored == []

    def test_limit_is_passed_through(self):
        repo = FakeRepo([
            _bt("A", "one two three four five six"),
            _bt("B", "one two three four five six"),
            _bt("C", "one two three four five six"),
        ])
        service = AnalysisService(repository=repo)
        # limit=2 -> only A and B compared -> exactly one pair
        count = service.run_comparison(min_score=0.5, limit=2)
        assert count == 1
