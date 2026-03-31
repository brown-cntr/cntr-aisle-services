"""
Tests for the ingestion CLI entrypoint.
"""
import sys
from unittest.mock import Mock

import pytest

from services.ingestion.src.cli import (
    _extract_legiscan_id_from_url,
    _parse_legiscan_bill_url,
    _resolve_bill_id_from_url,
)


class TestExtractLegiscanIdFromUrl:
    def test_extracts_id_from_simple_url(self):
        url = "https://legiscan.com/CA/bill/123456"
        assert _extract_legiscan_id_from_url(url) == 123456

    def test_extracts_id_from_url_with_trailing_segment(self):
        url = "https://legiscan.com/CA/bill/123456/2023"
        assert _extract_legiscan_id_from_url(url) == 123456

    def test_returns_none_for_non_legiscan_url(self):
        url = "https://example.com/CA/bill/123456"
        assert _extract_legiscan_id_from_url(url) is None

    def test_returns_none_for_non_numeric_id(self):
        url = "https://legiscan.com/CA/bill/notanid"
        assert _extract_legiscan_id_from_url(url) is None


class TestParseLegiscanBillUrl:
    def test_parses_valid_bill_url(self):
        url = "https://legiscan.com/IL/bill/SB3890/2025"
        parsed = _parse_legiscan_bill_url(url)
        assert parsed == ("IL", "SB3890", 2025)

    def test_returns_none_for_unexpected_format(self):
        url = "https://legiscan.com/IL/not-a-bill-url"
        assert _parse_legiscan_bill_url(url) is None

    def test_returns_none_for_invalid_year(self):
        url = "https://legiscan.com/IL/bill/SB3890/notayear"
        assert _parse_legiscan_bill_url(url) is None


class TestCliSingleBillIngestion:
    def test_ingests_single_bill_from_legiscan_url(self, monkeypatch, capsys):
        # Import inside test so monkeypatching affects the module instance used by main()
        from services.ingestion.src import cli

        # Arrange CLI arguments
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "prog",
                "--legiscan-url",
                "https://legiscan.com/CA/bill/123456",
            ],
        )

        # Mock dependencies used in single-bill path
        mock_client = Mock()
        mock_client.get_bill.return_value = {
            "bill_id": 123456,
            "state": "CA",
            "bill_number": "AB1",
            "title": "Test bill",
        }
        monkeypatch.setattr(cli, "LegiScanClient", Mock(return_value=mock_client))

        mock_bill = object()
        monkeypatch.setattr(cli, "parse_bill_data", Mock(return_value=mock_bill))

        mock_repo = Mock()
        mock_repo.store_bills.return_value = 1
        monkeypatch.setattr(cli, "BillsRepository", Mock(return_value=mock_repo))

        monkeypatch.setattr(cli, "get_supabase_client", Mock())

        # Ensure the normal ingestion path is not invoked
        ingest_mock = Mock()
        monkeypatch.setattr(
            cli, "IngestionService", Mock(return_value=Mock(ingest_ai_bills=ingest_mock))
        )

        # Act
        with pytest.raises(SystemExit) as excinfo:
            cli.main()

        # Assert
        assert excinfo.value.code == 0
        captured = capsys.readouterr()
        assert captured.out.strip() == "1"

        mock_client.get_bill.assert_called_once_with(123456)
        mock_repo.store_bills.assert_called_once_with([mock_bill])
        ingest_mock.assert_not_called()

    def test_exits_nonzero_for_invalid_legiscan_url(self, monkeypatch, capsys):
        from services.ingestion.src import cli

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "prog",
                "--legiscan-url",
                "https://not-legiscan.example.com/CA/bill/123456",
            ],
        )

        # Ensure we do not hit external services
        monkeypatch.setattr(cli, "LegiScanClient", Mock())

        with pytest.raises(SystemExit) as excinfo:
            cli.main()

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        # No strict assertion on stderr message, but CLI should not print a success count
        assert captured.out.strip() == ""


class TestResolveBillIdFromUrl:
    def test_resolves_bill_id_from_search_results(self, monkeypatch):
        from services.ingestion.src import cli

        mock_client = Mock()
        mock_client._make_request.return_value = {
            "status": "OK",
            "searchresult": {
                "summary": {"count": 1},
                "0": {
                    "state": "IL",
                    "bill_number": "SB3890",
                    "bill_id": 424242,
                    "url": "https://legiscan.com/IL/bill/SB3890/2025",
                },
            },
        }
        monkeypatch.setattr(cli, "LegiScanClient", Mock(return_value=mock_client))

        bill_id = _resolve_bill_id_from_url(
            "https://legiscan.com/IL/bill/SB3890/2025"
        )
        assert bill_id == 424242

    def test_raises_when_no_matching_results(self, monkeypatch):
        from services.ingestion.src import cli

        mock_client = Mock()
        mock_client._make_request.return_value = {
            "status": "OK",
            "searchresult": {
                "summary": {"count": 1},
                "0": {
                    "state": "IL",
                    "bill_number": "HB1234",
                    "bill_id": 111111,
                    "url": "https://legiscan.com/IL/bill/HB1234/2025",
                },
            },
        }
        monkeypatch.setattr(cli, "LegiScanClient", Mock(return_value=mock_client))

        with pytest.raises(ValueError):
            _resolve_bill_id_from_url("https://legiscan.com/IL/bill/SB3890/2025")

    def test_raises_when_multiple_matching_results(self, monkeypatch):
        from services.ingestion.src import cli

        mock_client = Mock()
        mock_client._make_request.return_value = {
            "status": "OK",
            "searchresult": {
                "summary": {"count": 2},
                "0": {
                    "state": "IL",
                    "bill_number": "SB3890",
                    "bill_id": 1,
                    "url": "https://legiscan.com/IL/bill/SB3890/2025",
                },
                "1": {
                    "state": "IL",
                    "bill_number": "SB3890",
                    "bill_id": 2,
                    "url": "https://legiscan.com/IL/bill/SB3890/2025",
                },
            },
        }
        monkeypatch.setattr(cli, "LegiScanClient", Mock(return_value=mock_client))

        with pytest.raises(ValueError):
            _resolve_bill_id_from_url("https://legiscan.com/IL/bill/SB3890/2025")


class TestCliWithNonNumericLegiscanUrl:
    def test_uses_resolver_when_url_has_no_numeric_id(self, monkeypatch, capsys):
        from services.ingestion.src import cli

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "prog",
                "--legiscan-url",
                "https://legiscan.com/IL/bill/SB3890/2025",
            ],
        )

        # Ensure resolver is used to obtain the bill_id
        monkeypatch.setattr(cli, "_resolve_bill_id_from_url", Mock(return_value=999999))

        # Mock ingestion for the resolved bill_id
        mock_client = Mock()
        mock_client.get_bill.return_value = {
            "bill_id": 999999,
            "state": "IL",
            "bill_number": "SB3890",
            "title": "Test bill",
        }
        monkeypatch.setattr(cli, "LegiScanClient", Mock(return_value=mock_client))

        mock_bill = object()
        monkeypatch.setattr(cli, "parse_bill_data", Mock(return_value=mock_bill))

        mock_repo = Mock()
        mock_repo.store_bills.return_value = 1
        monkeypatch.setattr(cli, "BillsRepository", Mock(return_value=mock_repo))

        monkeypatch.setattr(cli, "get_supabase_client", Mock())

        ingest_mock = Mock()
        monkeypatch.setattr(
            cli,
            "IngestionService",
            Mock(return_value=Mock(ingest_ai_bills=ingest_mock)),
        )

        with pytest.raises(SystemExit) as excinfo:
            cli.main()

        assert excinfo.value.code == 0
        captured = capsys.readouterr()
        assert captured.out.strip() == "1"

        cli._resolve_bill_id_from_url.assert_called_once_with(
            "https://legiscan.com/IL/bill/SB3890/2025"
        )
        mock_client.get_bill.assert_called_once_with(999999)
        mock_repo.store_bills.assert_called_once_with([mock_bill])
        ingest_mock.assert_not_called()

