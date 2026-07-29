"""Unit tests for full-text extraction and its wiring into LegiScanClient."""
import base64
from unittest.mock import Mock, patch

import pytest

from services.ingestion.src import text_extraction as te
from services.ingestion.src.legiscan_client import LegiScanClient


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


@pytest.fixture
def mock_api_key():
    return "test_api_key_12345"


@pytest.fixture
def client(mock_api_key):
    with patch("services.ingestion.src.legiscan_client.get_settings") as m:
        m.return_value = Mock(legiscan_api_key=mock_api_key)
        return LegiScanClient(api_key=mock_api_key)


class TestSelectLatestTextEntry:
    def test_picks_most_recent_by_date(self):
        entries = [
            {"doc_id": 1, "date": "2024-01-01"},
            {"doc_id": 3, "date": "2024-05-01"},
            {"doc_id": 2, "date": "2024-03-01"},
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 3

    def test_empty_returns_none(self):
        assert te.select_latest_text_entry([]) is None

    def test_ignores_non_dict_entries(self):
        entries = ["not-a-dict", {"doc_id": 7, "date": "2024-02-02"}]
        assert te.select_latest_text_entry(entries)["doc_id"] == 7

    def test_prefers_html_among_same_date_docs(self):
        # The Utah case: same version served as both PDF and HTML; prefer HTML.
        entries = [
            {"doc_id": 10, "date": "2024-06-01", "mime_id": 2, "mime": "application/pdf"},
            {"doc_id": 11, "date": "2024-06-01", "mime_id": 1, "mime": "text/html"},
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 11

    def test_html_preference_uses_mime_string_fallback(self):
        entries = [
            {"doc_id": 10, "date": "2024-06-01", "mime": "application/pdf"},
            {"doc_id": 11, "date": "2024-06-01", "mime": "text/html"},
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 11

    def test_xml_preferred_over_pdf(self):
        entries = [
            {"doc_id": 10, "date": "2024-06-01", "mime": "application/pdf"},
            {"doc_id": 11, "date": "2024-06-01", "mime": "text/xml"},
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 11

    def test_html_preferred_over_xml(self):
        entries = [
            {"doc_id": 10, "date": "2024-06-01", "mime": "application/xml"},
            {"doc_id": 11, "date": "2024-06-01", "mime_id": 1},
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 11

    def test_latest_version_wins_over_older_html(self):
        # Never swap to an older HTML version; the newest date always wins.
        entries = [
            {"doc_id": 1, "date": "2024-06-01", "mime_id": 2},  # newest, PDF
            {"doc_id": 2, "date": "2024-01-01", "mime_id": 1},  # older, HTML
        ]
        assert te.select_latest_text_entry(entries)["doc_id"] == 1


class TestNormalizeExtractedText:
    def test_normalizes_line_endings(self):
        assert te.normalize_extracted_text("a\r\nb\r\nc") == "a\nb\nc"

    def test_collapses_excess_blank_lines(self):
        assert te.normalize_extracted_text("a\n\n\n\n\nb") == "a\n\nb"

    def test_form_feed_becomes_blank_line(self):
        assert te.normalize_extracted_text("page1\fpage2") == "page1\n\npage2"

    def test_strips_trailing_whitespace_and_edges(self):
        assert te.normalize_extracted_text("  \n\nline   \nother\t\n\n  ") == "line\nother"

    def test_empty_passthrough(self):
        assert te.normalize_extracted_text("") == ""


class TestResolveTextPayload:
    def test_bare_text_object(self):
        payload = {"doc": "abc", "mime_id": 1}
        assert te._resolve_text_payload(payload) is payload

    def test_wrapped_text_key(self):
        inner = {"doc": "abc", "mime_id": 1}
        assert te._resolve_text_payload({"text": inner}) is inner

    def test_fully_wrapped_result(self):
        inner = {"doc": "abc", "mime_id": 1}
        assert te._resolve_text_payload({"result": {"text": inner}}) is inner

    def test_missing_text_raises(self):
        with pytest.raises(ValueError):
            te._resolve_text_payload({"nothing": "here"})


class TestExtractTextFromApiPayload:
    def test_html_extraction_and_state_normalization(self):
        # CA normalization rewrites <strike> to ~~...~~ markdown strikeout.
        html = "<html><body><p>Hello <strike>old</strike> world</p></body></html>"
        payload = {"text": {"doc": _b64(html), "mime_id": 1, "state_link": "http://x"}}
        mime_id, text, link = te.extract_text_from_api_payload(payload, state="CA")
        assert mime_id == 1
        assert "Hello" in text and "world" in text
        assert "~~old~~" in text
        assert link == "http://x"

    def test_pdf_without_pymupdf_returns_marker(self, monkeypatch):
        # The PDF extractors are optional. Force them to look absent so we exercise
        # the graceful "[PDF extraction unavailable]" marker deterministically,
        # regardless of whether pymupdf4llm/PyMuPDF happen to be installed here.
        monkeypatch.setattr(te, "pymupdf4llm", None)
        monkeypatch.setattr(te, "fitz", None)
        payload = {"text": {"doc": _b64("%PDF-1.4 fake"), "mime_id": 2}}
        _mime_id, text, _link = te.extract_text_from_api_payload(payload, state="US")
        assert text.startswith("[") and "PDF" in text

    def test_malformed_pdf_degrades_to_marker(self):
        # With the extractor installed, an undecodable PDF must still degrade to a
        # bracketed marker rather than raising.
        payload = {"text": {"doc": _b64("%PDF-1.4 fake"), "mime_id": 2}}
        _mime_id, text, _link = te.extract_text_from_api_payload(payload, state="US")
        assert text.startswith("[")

    def test_unsupported_mime_returns_marker(self):
        payload = {"text": {"doc": _b64("data"), "mime_id": 99}}
        _mime_id, text, _link = te.extract_text_from_api_payload(payload, state="US")
        assert text == "[Unsupported MIME type]"

    def test_missing_doc_raises(self):
        with pytest.raises(ValueError, match="missing `doc`"):
            te.extract_text_from_api_payload({"text": {"mime_id": 1}}, state="US")


class TestStateNormalization:
    def test_sc_inline_styles_to_markers(self):
        html = (
            "<html><body><p>"
            '<span style="text-decoration:line-through">old text</span>'
            '<span style="text-decoration:underline">new text</span>'
            "</p></body></html>"
        )
        payload = {"text": {"doc": _b64(html), "mime_id": 1}}
        _mime_id, text, _link = te.extract_text_from_api_payload(payload, state="SC")
        assert "[DEL]old text[/DEL]" in text
        assert "[INS]new text[/INS]" in text

    def test_tx_table_flattened_to_paragraph(self):
        html = (
            "<html><body><table>"
            "<tr><td>1</td><td>Sec 1. A covered entity shall comply.</td></tr>"
            "</table></body></html>"
        )
        payload = {"text": {"doc": _b64(html), "mime_id": 1}}
        _mime_id, text, _link = te.extract_text_from_api_payload(payload, state="TX")
        assert "Sec 1. A covered entity shall comply." in text


class TestExtractFullTextHelper:
    def test_returns_text(self):
        html = "<html><body><p>Body text</p></body></html>"
        payload = {"text": {"doc": _b64(html), "mime_id": 1}}
        assert "Body text" in te.extract_full_text(payload, state="US")

    def test_bad_payload_returns_none(self):
        assert te.extract_full_text({"nope": 1}, state="US") is None


class TestFetchBillFullText:
    def test_fetches_latest_and_extracts(self, client):
        html = "<html><body><p>Latest version</p></body></html>"
        bill_data = {
            "state": "US",
            "texts": [
                {"doc_id": 10, "date": "2024-01-01"},
                {"doc_id": 20, "date": "2024-06-01"},
            ],
        }
        with patch.object(client, "get_bill_text") as mock_text:
            mock_text.return_value = {"doc": _b64(html), "mime_id": 1}
            text = client.fetch_bill_full_text(bill_data)
        # Latest doc (doc_id=20) should be the one fetched.
        mock_text.assert_called_once_with(20)
        assert "Latest version" in text

    def test_no_texts_returns_none(self, client):
        assert client.fetch_bill_full_text({"state": "US", "texts": []}) is None

    def test_missing_doc_id_returns_none(self, client):
        bill_data = {"state": "US", "texts": [{"date": "2024-01-01"}]}
        assert client.fetch_bill_full_text(bill_data) is None

    def test_extraction_error_returns_none(self, client):
        bill_data = {"state": "US", "texts": [{"doc_id": 5, "date": "2024-01-01"}]}
        with patch.object(client, "get_bill_text") as mock_text:
            # Missing `doc` -> extract_text_from_api_payload raises ValueError -> None.
            mock_text.return_value = {"mime_id": 1}
            assert client.fetch_bill_full_text(bill_data) is None


class TestGetBillsIncludesText:
    @patch.object(LegiScanClient, "get_bill")
    def test_include_text_populates_full_text(self, mock_get_bill, client):
        bill_data = {
            "bill_id": 123456,
            "state": "CA",
            "bill_number": "AB123",
            "title": "AI Act",
            "year": 2024,
            "chamber": "Assembly",
            "status_date": "2024-01-15",
            "history": [{"date": "2024-01-15"}],
            "texts": [{"doc_id": 1, "date": "2024-01-15"}],
        }
        mock_get_bill.return_value = bill_data
        with patch.object(client, "fetch_bill_full_text", return_value="EXTRACTED") as mock_ft:
            bills = client.get_bills_from_search_results(
                [{"bill_id": 123456, "relevance": 90}], include_text=True
            )
        assert len(bills) == 1
        assert bills[0].full_text == "EXTRACTED"
        mock_ft.assert_called_once()

    @patch.object(LegiScanClient, "get_bill")
    def test_default_does_not_fetch_text(self, mock_get_bill, client):
        bill_data = {
            "bill_id": 123456,
            "state": "CA",
            "bill_number": "AB123",
            "title": "AI Act",
            "year": 2024,
            "chamber": "Assembly",
            "status_date": "2024-01-15",
            "history": [{"date": "2024-01-15"}],
        }
        mock_get_bill.return_value = bill_data
        with patch.object(client, "fetch_bill_full_text") as mock_ft:
            bills = client.get_bills_from_search_results(
                [{"bill_id": 123456, "relevance": 90}]
            )
        assert bills[0].full_text is None
        mock_ft.assert_not_called()
