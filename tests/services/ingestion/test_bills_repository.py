"""Unit tests for BillsRepository."""
from datetime import date, datetime
from uuid import uuid4

import pytest
from unittest.mock import Mock, patch

from services.ingestion.src.bills_repository import BillsRepository
from shared.models.bill import Bill, BillBody


@pytest.fixture
def mock_supabase():
    """Chainable mock Supabase client."""
    m = Mock()
    m.table.return_value = m
    m.select.return_value = m
    m.insert.return_value = m
    m.update.return_value = m
    m.eq.return_value = m
    m.in_.return_value = m
    m.order.return_value = m
    m.limit.return_value = m
    return m


@pytest.fixture
def repository(mock_supabase):
    """BillsRepository with mocked Supabase."""
    return BillsRepository(mock_supabase)


@pytest.fixture
def sample_bill():
    return Bill(
        external_id="123456",
        title="Test AI Regulation Bill",
        state="CA",
        year=2024,
        bill_number="AB123",
        body=BillBody.ASSEMBLY,
        summary="A test bill about AI regulation",
        version_date=date(2024, 1, 15),
    )


class TestBillsRepository:
    """Tests for BillsRepository."""

    def test_get_existing_bills_map(self, repository, mock_supabase):
        mock_supabase.execute.return_value.data = [
            {"external_id": "123456", "version_date": "2024-01-15", "updated_at": "2024-01-20T10:00:00Z"},
            {"external_id": "123457", "version_date": "2024-01-16", "updated_at": "2024-01-21T10:00:00Z"},
        ]
        result = repository.get_existing_bills_map()
        assert len(result) == 2
        assert "123456" in result
        assert result["123456"]["version_date"] == "2024-01-15"
        mock_supabase.table.assert_called_with("bills")

    def test_get_existing_bills_map_empty(self, repository, mock_supabase):
        mock_supabase.execute.return_value.data = []
        result = repository.get_existing_bills_map()
        assert result == {}

    def test_get_existing_legiscan_ids_in_list(self, repository, mock_supabase):
        mock_supabase.execute.return_value.data = [{"legiscan_id": 111}, {"legiscan_id": 333}]
        result = repository.get_existing_legiscan_ids_in_list([111, 222, 333, 444])
        assert result == {111, 333}
        mock_supabase.in_.assert_called_with("legiscan_id", [111, 222, 333, 444])

    def test_get_existing_legiscan_ids_in_list_empty(self, repository):
        result = repository.get_existing_legiscan_ids_in_list([])
        assert result == set()

    def test_store_bills_new_bill(self, repository, mock_supabase, sample_bill):
        mock_supabase.execute.return_value.data = []
        count = repository.store_bills([sample_bill])
        assert count == 1
        mock_supabase.insert.assert_called_once()

    def test_store_bills_existing_skip(self, repository, mock_supabase, sample_bill):
        mock_supabase.execute.return_value.data = [
            {"id": str(uuid4()), "external_id": "123456", "version_date": "2024-01-15"}
        ]
        count = repository.store_bills([sample_bill])
        assert count == 0
        mock_supabase.insert.assert_not_called()

    def test_store_bills_multiple(self, repository, mock_supabase):
        bills = [
            Bill(external_id="111", title="Bill 1", state="CA", year=2024, bill_number="AB111", body=BillBody.ASSEMBLY),
            Bill(external_id="222", title="Bill 2", state="NY", year=2024, bill_number="SB222", body=BillBody.SENATE),
        ]
        mock_supabase.execute.return_value.data = []
        count = repository.store_bills(bills)
        assert count == 2
        assert mock_supabase.insert.call_count == 2

    def test_store_bills_handles_errors(self, repository, mock_supabase, sample_bill):
        bills = [
            sample_bill,
            Bill(external_id="999", title="Bill 2", state="CA", year=2024, bill_number="AB999", body=BillBody.ASSEMBLY),
        ]
        mock_supabase.execute.side_effect = [
            Exception("Database error"),
            Mock(data=[]),
            Mock(data=[{"id": str(uuid4())}]),
        ]
        count = repository.store_bills(bills)
        assert count == 1

    def test_get_last_run_timestamp(self, repository, mock_supabase):
        mock_supabase.execute.return_value.data = [{"updated_at": "2024-01-20T10:00:00Z"}]
        result = repository.get_last_run_timestamp()
        assert result is not None
        assert isinstance(result, datetime)
        mock_supabase.order.assert_called_with("updated_at", desc=True)

    def test_get_last_run_timestamp_no_bills(self, repository, mock_supabase):
        mock_supabase.execute.return_value.data = []
        result = repository.get_last_run_timestamp()
        assert result is None
