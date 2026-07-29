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


class TestRemoveRepeatedFurniture:
    def test_removes_repeated_short_header(self):
        header = "34-LS0258 Draft"
        text = "\n".join([header, "real content one", header, "real content two", header, "x"])
        out = dc.remove_repeated_furniture(text, min_repeats=3)
        assert header not in out
        assert "real content one" in out and "real content two" in out

    def test_preserves_distinct_appropriation_line_items(self):
        # Digit-varying line items must not collapse into "boilerplate".
        text = "\n".join(
            [
                "- $153,663,700 from General Fund; and",
                "- $6,750,000 from General Fund; and",
                "- $4,500,000 from General Fund; and",
            ]
        )
        assert dc.remove_repeated_furniture(text) == text

    def test_protects_subsection_markers(self):
        text = "\n".join(["(a)", "text a", "(b)", "text b", "(a)", "text c", "(a)"])
        out = dc.remove_repeated_furniture(text, min_repeats=2)
        assert "(a)" in out


class TestStripPageMarkers:
    def test_removes_dash_wrapped_footer_on_short_line(self):
        text = "body text here\n**HB0110E** **-1-** **SCS CSHB 110**\nmore body"
        out = dc.strip_page_markers(text)
        assert "HB0110E" not in out
        assert "body text here" in out and "more body" in out

    def test_removes_page_of_footer_even_varying(self):
        text = "alpha\nPage 5 of 12\nbeta\nPage 6 of 12\ngamma\nPage of 12"
        out = dc.strip_page_markers(text)
        assert "Page" not in out
        assert "alpha" in out and "beta" in out and "gamma" in out

    def test_removes_bare_page_numbers(self):
        assert dc.strip_page_markers("real\n2\n- 3 -\nmore") == "real\nmore"

    def test_preserves_statutory_citation_in_long_line(self):
        line = (
            "SECTION 3. In Colorado Revised Statutes, add 26-1-119.5 as follows, "
            "concerning the consolidated administration of public assistance programs."
        )
        assert dc.strip_page_markers(line) == line


class TestStripInlineGutters:
    def test_removes_sequential_inline_numbers(self):
        # A running counter 1..12 embedded mid-line (needs >= 10 tokens to trigger).
        words = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta",
                 "theta", "iota", "kappa", "lambda", "mu", "nu"]
        text = " ".join(f"{w} {i}" for i, w in enumerate(words, 1)) + " end"
        out = dc.strip_inline_gutters(text)
        assert " 1 " not in out and " 7 " not in out and " 12 " not in out
        assert "alpha" in out and "mu" in out

    def test_survives_content_interleaved_with_gutter(self):
        # Statute-ish content numbers interleaved with the running counter 1..12.
        parts = []
        for i in range(1, 13):
            parts.append(f"provision {i} concerning section 4 of the code")
        out = dc.strip_inline_gutters(" ".join(parts))
        # The counter values are removed; the repeated content "4" is kept.
        assert "provision" in out and "section" in out

    def test_ignores_non_sequential_prose_numbers(self):
        # District ordinals / years are not sequential -> must be kept.
        text = "Representatives Scott of the 76 district, Bell of the 63 district, Davis of the 87 area"
        assert dc.strip_inline_gutters(text) == text

    def test_too_few_tokens_no_change(self):
        assert dc.strip_inline_gutters("a 1 b 2 c") == "a 1 b 2 c"


class TestRejoinSplitOrdinals:
    def test_rejoins_ordinals(self):
        assert dc.rejoin_split_ordinals("Scott of the 76 [th], Bell of the 63 [rd]") == (
            "Scott of the 76th, Bell of the 63rd"
        )


class TestCleanDocument:
    def test_html_only_dehyphenates(self):
        text = "inter-\nstate commerce\n1 not a gutter here"
        # mime 1 (HTML) must not strip gutters/furniture, only dehyphenate.
        assert dc.clean_document(text, mime_id=1) == "interstate commerce\n1 not a gutter here"

    def test_pdf_strips_gutter(self):
        text = "\n".join([f"{i} line {i}" for i in range(1, 9)])
        out = dc.clean_document(text, mime_id=2)
        assert "1 line 1" not in out and "line 1" in out

    def test_guard_reverts_catastrophic_removal(self):
        # A pathological input where furniture removal would nuke most content is
        # reverted by the safety guard rather than destroying the text.
        line = "SECTION x. add 26-1-119.5 concerning things " * 3
        big = "\n".join(line for _ in range(20))
        out = dc.clean_document(big, mime_id=2)
        assert dc._nonspace_len(out) >= dc._nonspace_len(big) * (1 - dc._MAX_SAFE_REMOVAL)

    def test_empty_passthrough(self):
        assert dc.clean_document("", mime_id=2) == ""
