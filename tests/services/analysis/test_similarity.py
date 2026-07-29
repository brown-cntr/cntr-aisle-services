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
