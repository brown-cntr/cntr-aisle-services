"""Load the LegiScan AI-search query from ``config/ai_search_query.yaml``.

Keeps the query terms editable and versioned outside Python. Falls back to
``DEFAULT_AI_SEARCH_QUERY`` if PyYAML is missing or the file is absent/malformed,
so ingestion never breaks on a bad config.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - exercised only in lean deploys
    yaml = None  # type: ignore

logger = logging.getLogger(__name__)

QUERY_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "ai_search_query.yaml"

# Fallback when the YAML cannot be read; kept in sync with config version 1.
DEFAULT_AI_SEARCH_QUERY = (
    "(digital NEAR replica) OR (computer-generated) OR (digital NEAR forger) OR "
    "(artificial NEAR intelligence) OR (automated NEAR decision NEAR making) OR "
    "(automatic NEAR decision NEAR making) OR (decision NEAR making NEAR tool) OR "
    "(automated NEAR decision NEAR tool) OR (automatic NEAR decision NEAR tool) OR "
    "(automated NEAR decision NEAR system) OR (automatic NEAR decision NEAR system) OR "
    "(automated NEAR final NEAR decision) OR (automatic NEAR final NEAR decision) OR "
    "(face NEAR recog) OR (facial NEAR recog) OR (voice NEAR recog) OR "
    "(iris NEAR recog) OR (gait NEAR recog) OR (genAI) OR (gen-AI) OR "
    "(generative NEAR AI) OR (generative NEAR tech) OR (generative NEAR model) OR "
    "(generative NEAR artificial) OR (machine NEAR learning) OR (deep NEAR learning) OR "
    "(chat NEAR bot) OR (virtual NEAR assistant) OR (ChatGPT) OR (Chat-GPT) OR "
    "(language NEAR model) OR (AI NEAR task NEAR force) OR (AI NEAR advis) OR "
    "(AI NEAR audit) OR (AI NEAR generate) OR (AI NEAR snoop) OR (deep NEAR fake) OR "
    "(synthetic NEAR media) OR (digital NEAR assistant) OR (natural NEAR language NEAR process) OR "
    "(computer NEAR vision) OR (frontier NEAR model) OR (software NEAR agent) OR "
    "(embodied NEAR robot) OR (foundation NEAR model) OR (LLM) OR (LLMs) OR "
    "(Information NEAR Technology NEAR Act)"
)


def build_query_from_groups(groups: List[str]) -> str:
    """OR-join clause bodies into a LegiScan boolean query: ``a``, ``b`` -> ``(a) OR (b)``."""
    return " OR ".join(f"({g.strip()})" for g in groups if g and g.strip())


def load_query_config(path: Optional[Path] = None) -> Dict[str, Any]:
    """Read and validate the query YAML (raises on problems; see get_ai_search_query)."""
    if yaml is None:
        raise ValueError("PyYAML is not installed; cannot read query config")

    cfg_path = path or QUERY_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not isinstance(data, dict):
        raise ValueError(f"Query config {cfg_path} is not a mapping")
    groups = data.get("groups")
    if not isinstance(groups, list) or not groups:
        raise ValueError(f"Query config {cfg_path} has no non-empty `groups` list")
    return data


def get_ai_search_query(path: Optional[Path] = None) -> str:
    """Return the assembled query, falling back to the built-in default on any error."""
    try:
        data = load_query_config(path)
    except (OSError, ValueError) as exc:
        logger.warning(
            "Falling back to built-in AI search query (could not load config): %s", exc
        )
        return DEFAULT_AI_SEARCH_QUERY

    query = build_query_from_groups(data["groups"])
    version = data.get("version", "?")
    logger.info("Loaded AI search query config version %s (%d groups)", version, len(data["groups"]))
    return query
