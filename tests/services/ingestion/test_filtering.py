"""Unit tests for filter_bills_for_processing (pure function)."""
import pytest

from services.ingestion.src.filtering import filter_bills_for_processing


class TestFilterBillsForProcessing:
    """Tests for filter_bills_for_processing."""

    def test_get_search_raw_returns_all(self):
        """When search results have no state/bill_number (getSearchRaw), return all."""
        existing = {"CA AB123": [{"external_id": "CA AB123 2024-01-15", "version_date": "2024-01-15"}]}
        search_results = [
            {"bill_id": "111", "relevance": 90},
            {"bill_id": "222", "relevance": 85},
        ]
        filtered = filter_bills_for_processing(
            search_results, existing, check_existing=False
        )
        assert len(filtered) == 2
        assert filtered[0]["bill_id"] == "111"
        assert filtered[1]["bill_id"] == "222"

    def test_get_search_with_state_number_includes_all_for_version_check(self):
        """When search has state/bill_number, all matching results are included (we fetch to check version_date)."""
        existing = {"CA AB123": [{"external_id": "CA AB123 2024-01-15", "version_date": "2024-01-15"}]}
        search_results = [
            {"bill_id": "123", "state": "CA", "bill_number": "AB123", "relevance": 90},
            {"bill_id": "999", "state": "CA", "bill_number": "AB999", "relevance": 85},
        ]
        filtered = filter_bills_for_processing(
            search_results, existing, check_existing=False
        )
        assert len(filtered) == 2
        bill_ids = [r["bill_id"] for r in filtered]
        assert "123" in bill_ids
        assert "999" in bill_ids

    def test_skips_results_without_state_or_bill_number(self):
        """Results missing state or bill_number are skipped when using getSearch-style data."""
        existing = {}
        search_results = [
            {"bill_id": "1", "state": "CA", "bill_number": "AB1", "relevance": 90},
            {"bill_id": "2", "state": "", "bill_number": "AB2", "relevance": 85},
            {"bill_id": "3", "state": "NY", "bill_number": "", "relevance": 80},
        ]
        filtered = filter_bills_for_processing(
            search_results, existing, check_existing=False, raw_search_results=False
        )
        assert len(filtered) == 1
        assert filtered[0]["bill_id"] == "1"
