"""Bill-to-bill text similarity for detecting model bills and derivative copies.

Scores a pair of bills by combining token-set Jaccard (shared vocabulary) with
k-gram shingle Jaccard (shared phrasing, which catches verbatim copied passages).
Both are cheap set operations, so a bill can be compared against the whole corpus
without heavy NLP deps. Operates on the ``full_text`` populated by ingestion.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Set, Tuple

from shared.models.bill_similarity import BillSimilarity, SimilarityClass

_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Bill text is formulaic, so thresholds are set to flag copying over boilerplate.
DEFAULT_MODEL_THRESHOLD = 0.60
DEFAULT_RELATED_THRESHOLD = 0.30
DEFAULT_SHINGLE_K = 3


@dataclass
class BillText:
    """Minimal view of a bill needed for comparison (external_id + full_text)."""
    external_id: str
    full_text: Optional[str]


def tokenize(text: Optional[str]) -> List[str]:
    """Lowercase word/number tokens; punctuation and case are discarded."""
    return _TOKEN_RE.findall((text or "").lower())


def shingles(tokens: Sequence[str], k: int = DEFAULT_SHINGLE_K) -> Set[Tuple[str, ...]]:
    """Return the set of contiguous k-token shingles (k-grams) from ``tokens``."""
    if k <= 0 or len(tokens) < k:
        return set()
    return {tuple(tokens[i : i + k]) for i in range(len(tokens) - k + 1)}


def jaccard(a: Set, b: Set) -> float:
    """Jaccard overlap |a ∩ b| / |a ∪ b|; 0.0 when both are empty."""
    if not a and not b:
        return 0.0
    union = len(a | b)
    if union == 0:
        return 0.0
    return len(a & b) / union


def text_similarity(
    text_a: Optional[str],
    text_b: Optional[str],
    k: int = DEFAULT_SHINGLE_K,
) -> float:
    """Combined 0.0-1.0 similarity: mean of token-set and k-gram shingle Jaccard.

    Returns 0.0 if either text is empty (nothing to compare).
    """
    tokens_a = tokenize(text_a)
    tokens_b = tokenize(text_b)
    if not tokens_a or not tokens_b:
        return 0.0
    set_score = jaccard(set(tokens_a), set(tokens_b))
    shingle_score = jaccard(shingles(tokens_a, k), shingles(tokens_b, k))
    return round((set_score + shingle_score) / 2.0, 4)


def classify(
    score: float,
    model_threshold: float = DEFAULT_MODEL_THRESHOLD,
    related_threshold: float = DEFAULT_RELATED_THRESHOLD,
) -> str:
    """Bucket a similarity score into a :class:`SimilarityClass` value."""
    if score >= model_threshold:
        return SimilarityClass.MODEL_OR_DERIVATIVE.value
    if score >= related_threshold:
        return SimilarityClass.RELATED.value
    return SimilarityClass.UNRELATED.value


def _make_similarity(a: BillText, b: BillText, score: float, k: int) -> BillSimilarity:
    return BillSimilarity(
        source_bill_id=a.external_id,
        target_bill_id=b.external_id,
        score=score,
        classification=classify(score),
        method=f"jaccard+{k}gram",
    )


def compare_bill_against(
    target: BillText,
    candidates: Sequence[BillText],
    *,
    min_score: float = DEFAULT_RELATED_THRESHOLD,
    top_k: Optional[int] = None,
    k: int = DEFAULT_SHINGLE_K,
) -> List[BillSimilarity]:
    """Score one bill against many; return matches >= ``min_score``, highest first.

    Skips the target itself (by external_id) and any candidate without text.
    """
    results: List[BillSimilarity] = []
    for cand in candidates:
        if cand.external_id == target.external_id:
            continue
        if not target.full_text or not cand.full_text:
            continue
        score = text_similarity(target.full_text, cand.full_text, k)
        if score >= min_score:
            results.append(_make_similarity(target, cand, score, k))
    results.sort(key=lambda r: r.score, reverse=True)
    return results[:top_k] if top_k is not None else results


def compare_all_pairs(
    bills: Sequence[BillText],
    *,
    min_score: float = DEFAULT_RELATED_THRESHOLD,
    k: int = DEFAULT_SHINGLE_K,
) -> List[BillSimilarity]:
    """Score every unique unordered pair; return matches >= ``min_score``.

    Each pair is emitted once (source_bill_id < target ordering by list index).
    Bills without text are skipped. O(n^2) in the number of bills.
    """
    results: List[BillSimilarity] = []
    n = len(bills)
    for i in range(n):
        a = bills[i]
        if not a.full_text:
            continue
        for j in range(i + 1, n):
            b = bills[j]
            if not b.full_text:
                continue
            score = text_similarity(a.full_text, b.full_text, k)
            if score >= min_score:
                results.append(_make_similarity(a, b, score, k))
    results.sort(key=lambda r: r.score, reverse=True)
    return results
