"""Unit tests for the analysis similarity engine."""
from services.analysis.src import similarity as sim
from services.analysis.src.similarity import BillText
from shared.models.bill_similarity import SimilarityClass


class TestPrimitives:
    def test_tokenize_lowercases_and_splits(self):
        assert sim.tokenize("The AI-Act, 2025!") == ["the", "ai", "act", "2025"]

    def test_tokenize_none(self):
        assert sim.tokenize(None) == []

    def test_shingles(self):
        toks = ["a", "b", "c", "d"]
        assert sim.shingles(toks, 3) == {("a", "b", "c"), ("b", "c", "d")}

    def test_shingles_shorter_than_k(self):
        assert sim.shingles(["a", "b"], 3) == set()

    def test_jaccard(self):
        assert sim.jaccard({1, 2, 3}, {2, 3, 4}) == 0.5
        assert sim.jaccard(set(), set()) == 0.0
        assert sim.jaccard({1}, {2}) == 0.0


class TestTextSimilarity:
    def test_identical_is_one(self):
        text = "the artificial intelligence safety and accountability act of 2025"
        assert sim.text_similarity(text, text) == 1.0

    def test_disjoint_is_zero(self):
        assert sim.text_similarity("alpha beta gamma delta", "one two three four") == 0.0

    def test_empty_is_zero(self):
        assert sim.text_similarity("", "anything here now") == 0.0
        assert sim.text_similarity(None, "anything here now") == 0.0

    def test_partial_overlap_between_zero_and_one(self):
        a = "the state shall regulate artificial intelligence systems in agencies"
        b = "the state shall regulate automated decision systems in agencies"
        score = sim.text_similarity(a, b)
        assert 0.0 < score < 1.0


class TestClassify:
    def test_bands(self):
        assert sim.classify(0.9) == SimilarityClass.MODEL_OR_DERIVATIVE.value
        assert sim.classify(0.6) == SimilarityClass.MODEL_OR_DERIVATIVE.value
        assert sim.classify(0.45) == SimilarityClass.RELATED.value
        assert sim.classify(0.3) == SimilarityClass.RELATED.value
        assert sim.classify(0.1) == SimilarityClass.UNRELATED.value


def _bt(ext, text):
    return BillText(external_id=ext, full_text=text)


class TestCompareBillAgainst:
    def test_excludes_self_and_filters_by_min_score(self):
        target = _bt("A", "the state shall regulate artificial intelligence systems")
        candidates = [
            _bt("A", "the state shall regulate artificial intelligence systems"),  # self
            _bt("B", "the state shall regulate artificial intelligence systems"),  # dup
            _bt("C", "completely different unrelated statutory language entirely"),
        ]
        results = sim.compare_bill_against(target, candidates, min_score=0.3)
        ids = [r.target_bill_id for r in results]
        assert "A" not in ids          # self excluded
        assert "B" in ids              # near-duplicate kept
        assert "C" not in ids          # below threshold dropped

    def test_top_k_and_sorted_desc(self):
        target = _bt("A", "one two three four five six")
        candidates = [
            _bt("B", "one two three four five six"),        # identical -> 1.0
            _bt("C", "one two three four five seven"),      # close
            _bt("D", "one two three ten eleven twelve"),    # weaker
        ]
        results = sim.compare_bill_against(target, candidates, min_score=0.0, top_k=2)
        assert len(results) == 2
        assert results[0].score >= results[1].score
        assert results[0].target_bill_id == "B"

    def test_skips_candidate_without_text(self):
        target = _bt("A", "one two three four")
        results = sim.compare_bill_against(target, [_bt("B", None)], min_score=0.0)
        assert results == []


class TestCompareAllPairs:
    def test_unique_pairs_only(self):
        bills = [
            _bt("A", "one two three four five"),
            _bt("B", "one two three four five"),
            _bt("C", "one two three four five"),
        ]
        results = sim.compare_all_pairs(bills, min_score=0.5)
        pairs = {(r.source_bill_id, r.target_bill_id) for r in results}
        # 3 bills -> 3 unique unordered pairs, each emitted once.
        assert pairs == {("A", "B"), ("A", "C"), ("B", "C")}

    def test_filters_and_skips_missing_text(self):
        bills = [
            _bt("A", "shared language about artificial intelligence regulation here"),
            _bt("B", "shared language about artificial intelligence regulation here"),
            _bt("C", None),
            _bt("D", "utterly distinct unrelated wording with nothing common"),
        ]
        results = sim.compare_all_pairs(bills, min_score=0.5)
        pairs = {(r.source_bill_id, r.target_bill_id) for r in results}
        assert pairs == {("A", "B")}
