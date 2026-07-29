"""Pytest configuration for analysis service tests."""
import sys
from pathlib import Path

import pytest

# Add project root to path so `import services.analysis.src` / `shared` works.
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True)
def setup_logging():
    """Silence logging during tests."""
    import logging
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)
