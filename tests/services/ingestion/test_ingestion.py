"""
Unit tests for IngestionService
"""
from uuid import uuid4

import pytest
from unittest.mock import Mock, patch

from services.ingestion.src.ingestion import IngestionService
from shared.models.bill import Bill, BillBody


@pytest.fixture
def mock_supabase ():
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
def mock_legiscan():
    return Mock()


@pytest.fixture
def service(mock_supabase, mock_legiscan):
    with patch("services.ingestion.src.ingestion.get_supabase_client") as m_get:
        m_get.return_value = mock_supabase
        with patch("services.ingestion.src.ingestion.LegiScanClient") as m_cls:
            m_cls.return_value = mock_legiscan
            svc = IngestionService()
            svc.legiscan_client = mock_legiscan
            return svc


class TestIngestionService:
    """Tests for IngestionService.ingest_ai_bills orchestration."""

    def test_ingest_ai_bills_full_workflow(self, service, mock_supabase, mock_legiscan):
        search_results = [{"bill_id": "111", "relevance": 90}, {"bill_id": "222", "relevance": 85}]
        mock_legiscan.search_ai_bills.return_value = (search_results, {"count": 2})
        mock_legiscan.get_bills_from_search_results.return_value = [
            Bill(external_id="111", title="Bill 111", state="CA", year=2024, bill_number="AB111", body=BillBody.ASSEMBLY),
            Bill(external_id="222", title="Bill 222", state="CA", year=2024, bill_number="AB222", body=BillBody.ASSEMBLY),
        ]
        mock_supabase.execute.return_value.data = []

        count = service.ingest_ai_bills(incremental=False)

        assert count == 2
        mock_legiscan.search_ai_bills.assert_called_once()
        mock_legiscan.get_bills_from_search_results.assert_called_once()
        assert mock_supabase.insert.call_count == 2

    @patch("services.ingestion.src.ingestion.LegiScanClient")
    @patch("services.ingestion.src.ingestion.get_supabase_client")
    def test_ingest_ai_bills_incremental_skips_existing(self, mock_get_supabase, mock_legiscan_class, mock_supabase):
        mock_legiscan = Mock()
        mock_legiscan_class.return_value = mock_legiscan
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value = mock_supabase
        mock_supabase.select.return_value = mock_supabase
        mock_supabase.insert.return_value = mock_supabase
        mock_supabase.update.return_value = mock_supabase
        mock_supabase.eq.return_value = mock_supabase
        mock_supabase.order.return_value = mock_supabase
        mock_supabase.limit.return_value = mock_supabase
        mock_supabase.in_.return_value = mock_supabase

        mock_legiscan.search_ai_bills.return_value = (
            [{"bill_id": "111", "relevance": 90}, {"bill_id": "222", "relevance": 85}],
            {"count": 2},
        )
        mock_legiscan.get_bills_from_search_results.return_value = [
            Bill(external_id="111", title="Bill 111", state="CA", year=2024, bill_number="AB111", body=BillBody.ASSEMBLY),
        ]
        mock_supabase.execute.side_effect = [
            Mock(data=[]),
            Mock(data=[]),
            Mock(data=[{"legiscan_id": 222}]),
            Mock(data=[]),
            Mock(data=[{"id": str(uuid4())}]),
        ]

        svc = IngestionService()
        count = svc.ingest_ai_bills(incremental=True)

        assert count == 1
        call_kw = mock_legiscan.get_bills_from_search_results.call_args[1]
        assert 222 in call_kw.get("existing_legiscan_ids", set())
