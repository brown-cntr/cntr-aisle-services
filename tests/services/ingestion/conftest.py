"""
Pytest configuration and shared fixtures for ingestion service tests
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture(autouse=True)
def setup_logging():
    """Disable logging during tests unless explicitly needed"""
    import logging
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def pytest_addoption(parser):
    """Add custom pytest option for integration tests"""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests that require real Supabase connection"
    )
