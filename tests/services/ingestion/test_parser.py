"""Unit tests for parser module: parse_bill_data and _map_chamber_to_body."""
from datetime import datetime, date

import pytest
from unittest.mock import patch

from services.ingestion.src.parser import parse_bill_data, _map_chamber_to_body
from shared.models.bill import Bill, BillBody


@pytest.fixture
def sample_bill_data():
    """Sample LegiScan bill dict for parsing."""
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


class TestParseBillData:
    """Tests for parse_bill_data."""

    def test_complete(self, sample_bill_data):
        bill = parse_bill_data(sample_bill_data)
        assert isinstance(bill, Bill)
        assert bill.external_id == "CA AB123 2024-01-15"
        assert bill.title == "An Act Relating to Artificial Intelligence Regulation"
        assert bill.state == "CA"
        assert bill.year == 2024
        assert bill.bill_number == "AB123"
        assert bill.body == BillBody.ASSEMBLY
        assert bill.version_date == date(2024, 1, 15)
        assert "legiscan.com/CA/bill/123456" in (bill.legiscan_url or "")

    def test_minimal(self):
        data = {
            "bill_id": 999999,
            "title": "Test Bill",
            "state": "NY",
            "bill_number": "SB100",
            "year": 2024,
        }
        bill = parse_bill_data(data)
        assert bill.external_id == "999999"
        assert bill.title == "Test Bill"
        assert bill.state == "NY"
        assert bill.year == 2024
        assert bill.bill_number == "SB100"
        assert bill.version_date is None
        assert bill.summary is None

    def test_year_from_session(self):
        data = {
            "bill_id": 123456,
            "title": "Test Bill",
            "state": "CA",
            "bill_number": "AB123",
            "session": {"session_title": "2023-2024 Regular Session"},
        }
        bill = parse_bill_data(data)
        assert bill.year == 2023

    def test_year_default(self):
        data = {"bill_id": 123456, "title": "Test Bill", "state": "CA", "bill_number": "AB123"}
        with patch("services.ingestion.src.parser.datetime") as m:
            m.now.return_value.year = 2024
            m.strptime = datetime.strptime
            bill = parse_bill_data(data)
        assert bill.year == 2024

    def test_version_date_from_history(self):
        data = {
            "bill_id": 123456,
            "title": "Test Bill",
            "state": "CA",
            "bill_number": "AB123",
            "year": 2024,
            "history": [{"date": "2024-02-20", "action": "Introduced"}],
        }
        bill = parse_bill_data(data)
        assert bill.version_date == date(2024, 2, 20)

    def test_invalid_date(self):
        data = {
            "bill_id": 123456,
            "title": "Test Bill",
            "state": "CA",
            "bill_number": "AB123",
            "year": 2024,
            "status_date": "invalid-date-format",
        }
        bill = parse_bill_data(data)
        assert bill.version_date is None


class TestMapChamberToBody:
    """Tests for _map_chamber_to_body."""

    def test_house(self):
        assert _map_chamber_to_body("House", "HB123") == BillBody.HOUSE
        assert _map_chamber_to_body("HOUSE", "") == BillBody.HOUSE
        assert _map_chamber_to_body("", "HB123") == BillBody.HOUSE
        assert _map_chamber_to_body("", "HR456") == BillBody.HOUSE

    def test_senate(self):
        assert _map_chamber_to_body("Senate", "SB123") == BillBody.SENATE
        assert _map_chamber_to_body("SENATE", "") == BillBody.SENATE
        assert _map_chamber_to_body("", "SB123") == BillBody.SENATE
        assert _map_chamber_to_body("", "SR456") == BillBody.SENATE

    def test_assembly(self):
        assert _map_chamber_to_body("Assembly", "AB123") == BillBody.ASSEMBLY
        assert _map_chamber_to_body("ASSEMBLY", "") == BillBody.ASSEMBLY
        assert _map_chamber_to_body("", "AB123") == BillBody.ASSEMBLY

    def test_default(self):
        assert _map_chamber_to_body("Unknown", "XYZ123") == BillBody.HOUSE

    def test_from_bill_number(self):
        assert _map_chamber_to_body("", "H123") == BillBody.HOUSE
        assert _map_chamber_to_body("", "S456") == BillBody.SENATE
        assert _map_chamber_to_body("", "A789") == BillBody.ASSEMBLY
