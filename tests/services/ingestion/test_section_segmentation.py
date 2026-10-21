"""Unit tests for bill section segmentation."""
from services.ingestion.src import section_segmentation as ss


class TestSegmentSections:
    def test_splits_on_section_markers_with_preamble(self):
        text = "Preamble line.\nSection 1. Do a thing.\nSection 2. Do another thing."
        secs = ss.segment_sections(text)
        assert [s["number"] for s in secs] == [None, "1", "2"]
        assert secs[0]["heading"] == ""  # preamble has no heading

    def test_recognizes_markdown_wrapped_and_abbreviated_markers(self):
        text = "*** Section 1.** AS 08.01 amended\n*** Sec. 2.** AS 08.02 amended"
        secs = ss.segment_sections(text)
        assert [s["number"] for s in secs] == ["1", "2"]

    def test_number_excludes_trailing_period(self):
        secs = ss.segment_sections("Section 3. Section 13-2-102 renumbered")
        assert secs[0]["number"] == "3"

    def test_code_style_numbers(self):
        assert ss.segment_sections("Section 1-101. text")[0]["number"] == "1-101"
        assert ss.segment_sections("Sec. 12A. text")[0]["number"] == "12A"

    def test_ignores_midsentence_and_plural_references(self):
        assert ss.segment_sections("under Sec. 5 of the Act")[0]["number"] is None
        assert ss.segment_sections("Sections 3 through 5 apply")[0]["number"] is None

    def test_no_markers_returns_single_preamble(self):
        secs = ss.segment_sections("just some text\nno sections here")
        assert len(secs) == 1 and secs[0]["number"] is None

    def test_empty_returns_empty_list(self):
        assert ss.segment_sections("   ") == []

    def test_roundtrip_preserves_content(self):
        text = "intro\nSection 1. alpha\nbeta\nSection 2. gamma"
        secs = ss.segment_sections(text)
        rejoined = "\n".join(s["text"] for s in secs)
        assert "".join(rejoined.split()) == "".join(text.split())
