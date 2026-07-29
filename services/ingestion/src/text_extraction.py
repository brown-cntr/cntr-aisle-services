"""Extract bill text from a LegiScan getBillText payload by MIME type.

HTML/PDF/WordPerfect/doc/RTF are supported. The extraction deps (bs4, markdownify,
pymupdf4llm/PyMuPDF, pypandoc) are optional; a missing one falls back to a
``[...]`` marker rather than raising, so the package imports fine without them.
"""

from __future__ import annotations

import base64
import os
import re
import tempfile
from collections.abc import Callable
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .document_cleaning import clean_document

try:
    from bs4 import BeautifulSoup  # type: ignore
except ImportError:  # pragma: no cover - exercised only in lean deploys
    BeautifulSoup = None  # type: ignore

try:
    # markdownify gives cleaner output than the plain-text fallback.
    from markdownify import markdownify as _markdownify  # type: ignore
except ImportError:
    _markdownify = None  # type: ignore

try:
    import pymupdf4llm  # type: ignore
except ImportError:
    pymupdf4llm = None  # type: ignore

try:
    # Fallback PDF text layer when pymupdf4llm is unavailable.
    import fitz  # type: ignore
except ImportError:
    fitz = None  # type: ignore

try:
    import pypandoc  # type: ignore
except ImportError:
    pypandoc = None  # type: ignore


SUPPORTED_MIME_LABELS = {
    1: "HTML",
    2: "PDF",
    3: "WordPerfect",
    4: "MS Word",
    5: "RTF",
    6: "DOCX",
}


def _html_to_text(html: str) -> str:
    """Convert HTML to text, preferring markdownify and falling back to bs4."""
    if _markdownify is not None:
        try:
            return _markdownify(html)
        except RecursionError:
            # Deeply nested HTML (e.g. NY budget bills) overflows markdownify's
            # recursive walk; bs4's flat extraction handles it.
            pass
    return BeautifulSoup(html, "html.parser").get_text("\n")


def _extract_html(raw_bytes: bytes, state: str) -> str:
    """Decode HTML bytes, normalize state markup quirks, and emit markdown text."""
    if BeautifulSoup is None:
        return "[HTML extraction unavailable: beautifulsoup4 is not installed]"

    # Imported lazily so this module loads even when bs4 is absent.
    from .text_normalization import normalize_strikethroughs

    html_text = raw_bytes.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html_text, "html.parser")
    # Normalize state-specific strikeout markup before markdown conversion.
    normalize_strikethroughs(soup, state)
    return _html_to_text(str(soup))


def _extract_pdf(raw_bytes: bytes, _state: str) -> str:
    """Convert a PDF to markdown via pymupdf4llm, falling back to the PyMuPDF text layer."""
    if pymupdf4llm is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(raw_bytes)
            tmp_path = tmp.name
        try:
            return pymupdf4llm.to_markdown(tmp_path)
        finally:
            # Windows may still hold the temp file open here; a failed cleanup
            # must not discard a good extraction.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    if fitz is not None:
        # Embedded text layer only; OCR is intentionally not automatic.
        pages = []
        with fitz.open(stream=raw_bytes, filetype="pdf") as document:
            for page in document:
                pages.append(page.get_text("text"))
        return "\n".join(pages)

    return "[PDF extraction unavailable: install pymupdf4llm or PyMuPDF]"


def _extract_via_pandoc(raw_bytes: bytes, source_format: str) -> str:
    """Convert a legacy document format to plain text through pandoc."""
    if pypandoc is None:
        return "[{} extraction unavailable: pypandoc is not installed]".format(source_format)
    return pypandoc.convert_text(
        raw_bytes.decode("latin-1", errors="ignore"), "plain", format=source_format
    )


def _extract_wpd(raw_bytes: bytes, _state: str) -> str:
    """Convert WordPerfect payloads to plain text through pandoc."""
    return _extract_via_pandoc(raw_bytes, "wpd")


def _extract_doc(raw_bytes: bytes, _state: str) -> str:
    """Convert legacy .doc payloads to plain text through pandoc."""
    return _extract_via_pandoc(raw_bytes, "doc")


def _extract_rtf(raw_bytes: bytes, _state: str) -> str:
    """Convert RTF payloads to plain text through pandoc."""
    return _extract_via_pandoc(raw_bytes, "rtf")


def _extract_docx(_raw_bytes: bytes, _state: str) -> str:
    """Return a deterministic marker for unsupported DOCX parsing."""
    return "[DOCX parsing not supported]"


_MIME_HANDLERS: Dict[int, Callable[[bytes, str], str]] = {
    1: _extract_html,
    2: _extract_pdf,
    3: _extract_wpd,
    4: _extract_doc,
    5: _extract_rtf,
    6: _extract_docx,
}


_MARKER_PREFIX = "["  # extraction markers like "[PDF extraction unavailable: ...]"

# Matches 3+ consecutive newlines (allowing trailing spaces on the blank lines).
_EXCESS_BLANK_LINES = re.compile(r"\n[ \t]*(?:\n[ \t]*){2,}\n")


def normalize_extracted_text(text: str) -> str:
    """Final whitespace tidy: normalize line endings, drop form feeds, collapse blanks.

    Structural cleanup (gutters, headers/footers) is handled earlier by
    ``document_cleaning.clean_document``.
    """
    if not text:
        return text
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\f", "\n\n").replace("\x0c", "\n\n")
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    text = _EXCESS_BLANK_LINES.sub("\n\n", text)
    return text.strip("\n")


def extract_text_from_mime(raw_bytes: bytes, mime_id: int, state: str) -> str:
    """Route raw document bytes to the extractor registered for the given MIME id."""
    handler = _MIME_HANDLERS.get(mime_id)
    if handler is None:
        return "[Unsupported MIME type]"
    try:
        extracted = handler(raw_bytes, state)
    except Exception as exc:
        # Return an explicit marker so batch runs continue and failures stay traceable.
        return f"[MIME extraction error: {exc}]"
    # Leave "[...unavailable...]" markers untouched; only clean real content.
    if extracted.startswith(_MARKER_PREFIX):
        return extracted
    # Format/state-aware structural cleanup, then a final whitespace tidy.
    return normalize_extracted_text(clean_document(extracted, mime_id, state=state))


def _resolve_text_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize accepted LegiScan payload shapes to a single text object.

    Accepts:
    - a bare text object: ``{"doc": ..., "mime_id": ...}`` (what
      ``LegiScanClient.get_bill_text`` returns directly),
    - a wrapped getBillText payload: ``{"text": {...}}``,
    - a fully wrapped API response: ``{"result": {"text": {...}}}``.
    """
    # Bare text object (the shape LegiScanClient.get_bill_text hands back).
    if "doc" in payload or "mime_id" in payload:
        return payload

    if "text" in payload and isinstance(payload["text"], dict):
        return payload["text"]

    if "result" in payload and isinstance(payload["result"], dict):
        nested = payload["result"].get("text")
        if isinstance(nested, dict):
            return nested

    raise ValueError("LegiScan text payload missing `text` object")


def extract_text_from_api_payload(payload: Dict[str, Any], state: str) -> Tuple[int, str, str]:
    """Decode a getBillText response and return MIME id, extracted text, and source link."""
    text_payload = _resolve_text_payload(payload)

    encoded_doc = text_payload.get("doc")
    if not encoded_doc:
        raise ValueError("LegiScan text payload is missing `doc`")

    mime_id = int(text_payload.get("mime_id", -1))
    raw_bytes = base64.b64decode(encoded_doc)
    extracted = extract_text_from_mime(raw_bytes, mime_id, state)
    state_link = str(text_payload.get("state_link", ""))

    return mime_id, extracted, state_link


def select_latest_text_entry(entries: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pick the most recent text entry from a bill's ``texts`` list by its date field."""
    valid: List[Dict[str, Any]] = [e for e in entries if isinstance(e, dict)]
    if not valid:
        return None
    return max(valid, key=lambda entry: str(entry.get("date", "")))


def extract_full_text(text_payload: Dict[str, Any], state: str) -> Optional[str]:
    """Convenience wrapper: return just the extracted text for a getBillText payload.

    Returns ``None`` when the payload cannot be resolved into document bytes.
    """
    try:
        _mime_id, text, _link = extract_text_from_api_payload(text_payload, state=state)
    except ValueError:
        return None
    return text or None
