"""Unit tests for the shared, format-aware document cleaning pass."""
from services.ingestion.src import document_cleaning as dc


class TestStripLineNumberGutters:
    def test_strips_sequential_gutter_with_reset(self):
        text = "\n".join(f"{i} content line {i}" for i in range(1, 9))
        out = dc.strip_line_number_gutters(text)
        assert "content line 1" in out
        assert not any(line.strip().startswith(("1 ", "2 ")) for line in out.split("\n"))

    def test_keeps_markdown_prefix_when_stripping(self):
        text = "\n".join(["## 1 Heading text"] + [f"{i} body {i}" for i in range(2, 7)])
        out = dc.strip_line_number_gutters(text)
        assert out.split("\n")[0] == "## Heading text"

    def test_does_not_strip_non_gutter_numbers(self):
        # A couple of coincidental leading numbers must not be treated as a gutter.
        text = "5 U.S.C. 552 applies here.\nThis is ordinary prose.\nAnother line."
        assert dc.strip_line_number_gutters(text) == text

    def test_drops_bare_wrapped_gutter_number(self):
        text = "1 first\n2\n3 third\n4 fourth\n5 fifth\n6 sixth"
        out = dc.strip_line_number_gutters(text)
        assert "\n2\n" not in "\n" + out + "\n"


class TestCleanDocument:
    def test_html_only_dehyphenates(self):
        text = "inter-\nstate commerce\n1 not a gutter here"
        # mime 1 (HTML) must not strip gutters/furniture, only dehyphenate.
        assert dc.clean_document(text, mime_id=1) == "interstate commerce\n1 not a gutter here"

    def test_pdf_strips_gutter(self):
        text = "\n".join([f"{i} line {i}" for i in range(1, 9)])
        out = dc.clean_document(text, mime_id=2)
        assert "1 line 1" not in out and "line 1" in out

    def test_empty_passthrough(self):
        assert dc.clean_document("", mime_id=2) == ""
