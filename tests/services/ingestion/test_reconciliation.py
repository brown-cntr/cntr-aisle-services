"""Unit tests for OpenStates<->LegiScan reconciliation."""
from datetime import date

from services.ingestion.src import reconciliation as rec
from shared.models.bill import Bill, BillBody, BillSource


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


class TestReconcilePair:
    def test_merges_openstates_ids_and_fills_gaps(self):
        legi = _bill(bill_number="SB53", legiscan_id=111, summary=None, url=None)
        os = _bill(bill_number="S 53", openstates_id="ocd-bill/abc",
                   openstates_url="https://openstates.org/ca/bills/sb53",
                   summary="An AI bill", url="https://leginfo.ca.gov/sb53")
        merged = rec.reconcile_pair(legi, os)
        assert merged.legiscan_id == 111
        assert merged.openstates_id == "ocd-bill/abc"
        assert merged.openstates_url == "https://openstates.org/ca/bills/sb53"
        assert merged.summary == "An AI bill"  # filled from OpenStates
        assert merged.url == "https://leginfo.ca.gov/sb53"
        assert merged.source == BillSource.BOTH.value

    def test_legiscan_fields_win(self):
        legi = _bill(bill_number="SB53", summary="Legi summary")
        os = _bill(bill_number="S 53", summary="OS summary")
        merged = rec.reconcile_pair(legi, os)
        assert merged.summary == "Legi summary"


class TestReconcileBills:
    def test_matches_labels_and_stats(self):
        legi_match = _bill(bill_number="SB53", legiscan_id=1,
                           version_date=date(2025, 1, 7))
        legi_only = _bill(bill_number="AB100", legiscan_id=2, state="CA")
        os_match = _bill(bill_number="S 53", openstates_id="os-1",
                         version_date=date(2025, 1, 8))
        os_only = _bill(bill_number="HR 900", openstates_id="os-2", state="CA")

        merged, stats = rec.reconcile_bills(
            [legi_match, legi_only], [os_match, os_only]
        )

        by_source: dict[str, list[Bill]] = {}
        for b in merged:
            by_source.setdefault(b.source, []).append(b)

        assert stats == {
            "legiscan_total": 2,
            "openstates_total": 2,
            "matched": 1,
            "legiscan_only": 1,
            "openstates_only": 1,
            "total": 3,
        }
        assert len(by_source[BillSource.BOTH.value]) == 1
        assert by_source[BillSource.BOTH.value][0].legiscan_id == 1
        assert by_source[BillSource.BOTH.value][0].openstates_id == "os-1"
        assert len(by_source[BillSource.LEGISCAN.value]) == 1
        assert len(by_source[BillSource.OPENSTATES.value]) == 1

    def test_one_legiscan_matches_only_one_openstates(self):
        # Two OpenStates rows with same number should not both claim one LegiScan bill.
        legi = _bill(bill_number="SB53", legiscan_id=1, version_date=date(2025, 1, 7))
        os_a = _bill(bill_number="S 53", openstates_id="a", version_date=date(2025, 1, 7))
        os_b = _bill(bill_number="S 53", openstates_id="b", version_date=date(2025, 1, 7))
        merged, stats = rec.reconcile_bills([legi], [os_a, os_b])
        assert stats["matched"] == 1
        assert stats["openstates_only"] == 1
