"""CLI entrypoint for ingestion service"""
import argparse
import logging
import sys
from datetime import date, datetime

from .ingestion import IngestionService

logger = logging.getLogger(__name__)


def main():
    """Main entry point for ingestion service."""
    parser = argparse.ArgumentParser(
        description="Ingest AI-related bills from LegiScan"
    )
    parser.add_argument(
        "--min-relevance",
        type=int,
        default=0,
        help="Minimum relevance score (0-100, default: 0)",
    )
    parser.add_argument(
        "--state",
        type=str,
        default="ALL",
        help="State code (e.g., CA, NY) or 'ALL' for all states (default: ALL)",
    )
    parser.add_argument(
        "--use-raw",
        action="store_true",
        default=True,
        help="Use getSearchRaw for bulk results (default: True)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Run full ingestion (process all bills, including existing ones, default: False)",
    )
    parser.add_argument(
        "--check-existing",
        action="store_true",
        default=False,
        help="Also check existing bills for updates (slower but catches all changes, default: False)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Search and fetch bills but do not insert or update in the database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only process N bills (for testing)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        metavar="DATE",
        help="Only store bills with version_date on or after this date (ISO date or datetime, e.g. 2026-01-27 or 2026-01-27T12:00:00+00:00)",
    )

    args = parser.parse_args()

    # Parse --since to date if provided
    since_date = None
    if args.since:
        since_str = args.since.strip().replace("Z", "+00:00")
        try:
            since_date = date.fromisoformat(since_str)
        except ValueError:
            since_date = datetime.fromisoformat(since_str).date()

    # Setup logging 
    from shared.utils.config import get_settings
    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        service = IngestionService()
        count = service.ingest_ai_bills(
            min_relevance=args.min_relevance,
            state=args.state,
            use_raw=args.use_raw,
            incremental=not args.full,
            check_existing=args.check_existing,
            since_date=since_date,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        print(count)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)
