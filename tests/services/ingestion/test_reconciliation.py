"""Unit tests for OpenStates<->LegiScan reconciliation."""
from datetime import date

from services.ingestion.src import reconciliation as rec
from shared.models.bill import Bill, BillBody


def _bill(**kw) -> Bill:
    base = dict(
        external_id=kw.get("external_id", "X"),
        title=kw.get("title", "A Bill"),
        state=kw.get("state", "CA"),
        year=kw.get("year", 2025),
        bill_number=kw.get("bill_number", "SB53"),
        body=kw.get("body", BillBody.SENATE),
    )
    base.update({k: v for k, v in kw.items() if k in Bill.model_fields})
    return Bill(**base)


class TestIdentifierNormalization:
    def test_openstates_strips_space_and_session_prefix(self):
        # "S 53" -> SB rule, "25-" regex stripped, spaces removed.
        assert rec.normalize_openstates_identifier("S 53") == "SB53"
        assert rec.normalize_openstates_identifier("HR 1234") == "HB1234"

    def test_legiscan_strips_zero_padding(self):
        assert rec.normalize_legiscan_bill_number("HR0001234") == "HB1234"
        assert rec.normalize_legiscan_bill_number("A00") == "A"

    def test_open_and_legi_converge(self):
        assert rec.normalize_openstates_identifier("HR 1234") == rec.normalize_legiscan_bill_number(
            "HR0001234"
        )


class TestCombinedIdentifier:
    def test_builds_key(self):
        assert rec.combined_identifier("SB53", "ca", "2025-01-07") == "SB53_CA_2025-01-07"

    def test_missing_date_uses_placeholder(self):
        assert rec.combined_identifier("SB53", "CA", None) == "SB53_CA_N/A"


class TestDatesAndSimilarity:
    def test_within_range(self):
        assert rec.dates_within_range("2025-01-07", "2025-01-09", days=3) is True
        assert rec.dates_within_range("2025-01-07", "2025-01-20", days=3) is False

    def test_within_range_accepts_date_objects(self):
        assert rec.dates_within_range(date(2025, 1, 7), date(2025, 1, 7)) is True

    def test_invalid_dates_false(self):
        assert rec.dates_within_range(None, "2025-01-07") is False

    def test_similarity_identical(self):
        assert rec.similarity("AI Safety Act", "AI Safety Act") == 100.0

    def test_similarity_different(self):
        assert rec.similarity("apples", "zzzzzz") < 30.0


class TestIsSameBill:
    def test_match_on_number_state_and_close_date(self):
        legi = _bill(bill_number="SB53", version_date=date(2025, 1, 7))
        os = _bill(bill_number="S 53", version_date=date(2025, 1, 9))
        assert rec.is_same_bill(legi, os) is True

    def test_no_match_different_state(self):
        legi = _bill(state="CA", bill_number="SB53", version_date=date(2025, 1, 7))
        os = _bill(state="NY", bill_number="S 53", version_date=date(2025, 1, 7))
        assert rec.is_same_bill(legi, os) is False

    def test_no_match_different_number(self):
        legi = _bill(bill_number="SB53")
        os = _bill(bill_number="S 99")
        assert rec.is_same_bill(legi, os) is False

    def test_far_dates_still_match_on_high_title_similarity(self):
        legi = _bill(bill_number="SB53", title="Frontier AI Safety Act",
                     version_date=date(2025, 1, 1))
        os = _bill(bill_number="S 53", title="Frontier AI Safety Act",
                   version_date=date(2025, 6, 1))
        assert rec.is_same_bill(legi, os) is True
