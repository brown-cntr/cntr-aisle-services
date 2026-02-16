"""Unit tests for LegiScanClient."""
import json
import urllib.error

import pytest
from unittest.mock import Mock, patch

from services.ingestion.src.legiscan_client import LegiScanClient
from shared.models.bill import Bill, BillBody


@pytest.fixture
def mock_api_key():
    return "test_api_key_12345"


@pytest.fixture
def client(mock_api_key):
    with patch("services.ingestion.src.legiscan_client.get_settings") as m:
        m.return_value = Mock(legiscan_api_key=mock_api_key)
        return LegiScanClient(api_key=mock_api_key)


@pytest.fixture
def sample_search_response():
    return {
        "status": "OK",
        "searchresult": {
            "summary": {"count": 3, "relevancy": "85-95"},
            "results": [
                {"bill_id": 123456, "relevance": 90, "state": "CA", "bill_number": "AB123", "title": "AI Regulation Act"},
                {"bill_id": 123457, "relevance": 88, "state": "NY", "bill_number": "SB456", "title": "Machine Learning Oversight"},
                {"bill_id": 123458, "relevance": 75, "state": "TX", "bill_number": "HB789", "title": "Technology Policy"},
            ],
        },
    }


@pytest.fixture
def sample_bill_data():
    return {
        "bill_id": 123456,
        "state": "CA",
        "bill_number": "AB123",
        "title": "An Act Relating to Artificial Intelligence Regulation",
        "description": "This bill regulates the use of artificial intelligence in state agencies.",
        "year": 2024,
        "chamber": "Assembly",
        "status_date": "2024-01-15",
        "url": "https://legiscan.com/CA/bill/123456",
        "session": {"session_id": 2024, "session_title": "2024 Regular Session"},
        "history": [{"date": "2024-01-15", "action": "Introduced"}],
    }


class TestLegiScanClient:
    """Tests for LegiScanClient."""

    def test_init_with_api_key(self, mock_api_key):
        with patch("services.ingestion.src.legiscan_client.get_settings"):
            c = LegiScanClient(api_key=mock_api_key)
            assert c.api_key == mock_api_key
            assert c.base_url == "https://api.legiscan.com"
            assert c.request_count == 0

    def test_init_without_api_key_fails(self):
        with patch("services.ingestion.src.legiscan_client.get_settings") as m:
            m.return_value = Mock(legiscan_api_key=None)
            with patch.dict("os.environ", {}, clear=True):
                with pytest.raises(ValueError, match="LEGISCAN_API_KEY not found"):
                    LegiScanClient()

    def test_init_from_env(self):
        with patch("services.ingestion.src.legiscan_client.get_settings") as m:
            m.return_value = Mock(legiscan_api_key=None)
            with patch.dict("os.environ", {"LEGISCAN_API_KEY": "env_key"}):
                c = LegiScanClient()
                assert c.api_key == "env_key"

    @patch("urllib.request.urlopen")
    def test_make_request_success(self, mock_urlopen, client, sample_search_response):
        mock_urlopen.return_value = Mock(read=Mock(return_value=json.dumps(sample_search_response).encode()))
        result = client._make_request("getSearchRaw", query="test", state="CA")
        assert result["status"] == "OK"
        assert "searchresult" in result
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_make_request_api_error(self, mock_urlopen, client):
        err = {"status": "ERROR", "alert": {"message": "Invalid API key"}}
        mock_urlopen.return_value = Mock(read=Mock(return_value=json.dumps(err).encode()))
        with pytest.raises(Exception, match="LegiScan API error"):
            client._make_request("getBill", id=123456)

    @patch("urllib.request.urlopen")
    def test_make_request_429_retry(self, mock_urlopen, client, sample_search_response):
        http_err = urllib.error.HTTPError("https://api.legiscan.com/", 429, "Rate Limit", {}, None)
        resp = Mock(read=Mock(return_value=json.dumps(sample_search_response).encode()))
        mock_urlopen.side_effect = [http_err, resp]
        with patch("time.sleep"):
            result = client._make_request("getBill", id=123456)
        assert result["status"] == "OK"
        assert mock_urlopen.call_count == 2

    @patch.object(LegiScanClient, "_make_request")
    def test_search_ai_bills(self, mock_req, client, sample_search_response):
        mock_req.return_value = sample_search_response
        results, summary = client.search_ai_bills(min_relevance=85, state="ALL")
        assert len(results) == 2
        assert all(b["relevance"] >= 85 for b in results)
        assert summary["count"] == 3

    @patch.object(LegiScanClient, "_make_request")
    def test_get_bill(self, mock_req, client, sample_bill_data):
        mock_req.return_value = {"status": "OK", "bill": sample_bill_data}
        result = client.get_bill(123456)
        assert result["bill_id"] == 123456
        mock_req.assert_called_once_with("getBill", id=123456)

    @patch.object(LegiScanClient, "get_bill")
    @patch("services.ingestion.src.legiscan_client.parse_bill_data")
    def test_get_bills_from_search_results(self, mock_parse, mock_get_bill, client, sample_bill_data):
        mock_get_bill.return_value = sample_bill_data
        mock_parse.return_value = Bill(
            external_id="CA AB123 2024-01-15",
            title="Test",
            state="CA",
            year=2024,
            bill_number="AB123",
            body=BillBody.ASSEMBLY,
        )
        results = [{"bill_id": 123456, "relevance": 90}, {"bill_id": 123457, "relevance": 88}]
        bills = client.get_bills_from_search_results(results)
        assert len(bills) == 2
        assert mock_get_bill.call_count == 2
        assert mock_parse.call_count == 2

    @patch.object(LegiScanClient, "get_bill")
    def test_get_bills_from_search_results_handles_fetch_error(self, mock_get_bill, client, sample_bill_data):
        mock_get_bill.side_effect = [sample_bill_data, Exception("Bill not found"), sample_bill_data]
        results = [{"bill_id": 123456, "relevance": 90}, {"bill_id": 999999, "relevance": 88}, {"bill_id": 123457, "relevance": 85}]
        bills = client.get_bills_from_search_results(results)
        assert len(bills) == 2
        assert mock_get_bill.call_count == 3

    @patch.object(LegiScanClient, "get_bill")
    def test_get_bills_from_search_results_skips_existing_ids(self, mock_get_bill, client, sample_bill_data):
        mock_get_bill.return_value = sample_bill_data
        results = [{"bill_id": 123456, "relevance": 90}, {"bill_id": 123457, "relevance": 88}, {"bill_id": 123458, "relevance": 85}]
        bills = client.get_bills_from_search_results(results, existing_legiscan_ids={123457})
        assert len(bills) == 2
        call_ids = [c[0][0] for c in mock_get_bill.call_args_list]
        assert 123457 not in call_ids

    @patch.object(LegiScanClient, "get_bill")
    def test_get_bills_from_search_results_missing_bill_id(self, mock_get_bill, client, sample_bill_data):
        mock_get_bill.return_value = sample_bill_data
        results = [{"bill_id": 123456, "relevance": 90}, {"relevance": 88}, {"bill_id": 123457, "relevance": 85}]
        bills = client.get_bills_from_search_results(results)
        assert len(bills) == 2
        assert mock_get_bill.call_count == 2

    def test_ai_search_query_defined(self, client):
        assert client.AI_SEARCH_QUERY
        assert "artificial NEAR intelligence" in client.AI_SEARCH_QUERY

    @patch("urllib.request.urlopen")
    def test_rate_limiting_delay(self, mock_urlopen, client, sample_search_response):
        mock_urlopen.return_value = Mock(read=Mock(return_value=json.dumps(sample_search_response).encode()))
        with patch("time.sleep") as mock_sleep:
            client._make_request("getBill", id=1)
            client._make_request("getBill", id=2)
            assert mock_sleep.called
