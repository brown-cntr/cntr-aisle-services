"""CLI entrypoint for the analysis service."""
import argparse
import logging
import sys

from .comparison import AnalysisService
from .similarity import DEFAULT_RELATED_THRESHOLD, DEFAULT_SHINGLE_K

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Compute bill-to-bill text similarity (model-bill detection)"
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_RELATED_THRESHOLD,
        help=f"Minimum similarity score to record (0.0-1.0, default: {DEFAULT_RELATED_THRESHOLD})",
    )
    parser.add_argument(
        "--shingle-k",
        type=int,
        default=DEFAULT_SHINGLE_K,
        help=f"k-gram size for shingle overlap (default: {DEFAULT_SHINGLE_K})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only compare the first N bills that have full_text (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Compute similarities but do not write them to the database",
    )
    args = parser.parse_args()

    from shared.utils.config import get_settings

    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        service = AnalysisService()
        count = service.run_comparison(
            min_score=args.min_score,
            k=args.shingle_k,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        print(count)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        sys.exit(1)
