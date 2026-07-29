# Bill text processing: notes and decisions

Working notes for the ingestion text-processing cleanup (Wilber, with Product).
Covers the format-selection fix, query centralization, artifact cleanup, the
LOCUS and OpenStates investigation, and bill-text versioning.

## 1. Why PDFs were parsed instead of available HTML (e.g. Utah)

Root cause: `select_latest_text_entry` (in `services/ingestion/src/text_extraction.py`)
picked the text document with the newest `date` and ignored the document format.
LegiScan frequently exposes the same bill version as several documents that share
one `date`: for many states that includes both an HTML and a PDF. When the PDF
happened to sort first, the pipeline extracted from the PDF even though an
equivalent, cleaner HTML existed for that same version. Utah enrolled bills are a
recurring example.

Why it matters: the HTML path (`markdownify` / BeautifulSoup) preserves headings,
sections, and strikeout markup, and the repo already has substantial
state-specific HTML normalization in `text_normalization.py` (NY, WV, AZ, CA,
DE/NH, IL, NJ, SC, TX strikeout/insert handling). The PDF path
(`pymupdf4llm` / PyMuPDF text layer) has none of that and adds line-number
gutters, page headers/footers, and broken paragraphs. Choosing PDF throws away
the normalization investment and produces noisier text.

### Fix (implemented)

Format-aware selection while keeping "latest version wins":

- Group a bill's `texts` by `date` (LegiScan's version key).
- Take the newest date. We never fall back to an older version to get a nicer
  format (per the team decision).
- Within that newest version, rank documents by `_FORMAT_PREFERENCE` = HTML, then
  XML, then everything else, and pick the best. Ranking reads both `mime_id`
  (1 == HTML) and the `mime` string (`text/html`, `application/xml`, ...).

This directly implements the rule of thumb: prioritize HTML/XML when available.
To change the preference order (or add a format), edit `_FORMAT_PREFERENCE`.

### LegiScan alternative-format note

LegiScan's `getBillText` returns one document per `doc_id` with a fixed
`mime_id`; there is no "give me this doc as HTML" parameter. The choice therefore
has to happen at selection time across the `texts` list, which is what the fix
does. `SUPPORTED_MIME_LABELS` documents the known mime ids (1 HTML, 2 PDF,
3 WordPerfect, 4 MS Word, 5 RTF, 6 DOCX). XML is not a standard LegiScan mime id
today, so the XML preference keys off the `mime` string and is future-proofing.

## 2. Centralized, editable query list

The `AI_SEARCH_QUERY` was a hardcoded ~48-term string in `legiscan_client.py`.
It now lives in `services/ingestion/config/ai_search_query.yaml`:

- `groups`: one clause body per line; the loader wraps each in `(...)` and
  OR-joins them. Editing coverage no longer touches Python.
- `version` + `updated` + `changelog`: query versioning. Bump and log on change.
- `suggested_groups`: staging area so anyone can *suggest-add* a keyword without
  affecting the live query. Promote to `groups` to activate.

`services/ingestion/src/query_config.py` loads and assembles the query, and falls
back to a built-in default (identical to config v1) if PyYAML is missing or the
file is unreadable, so ingestion never breaks on a bad config. The extraction was
byte-for-byte identical to the previous hardcoded string, so behavior is
unchanged until someone edits the YAML.

## 3. Improving artifact outputs

Implemented now: a conservative, format-agnostic `normalize_extracted_text` pass
applied to all extractor output (drops form-feed page breaks, normalizes line
endings, strips trailing whitespace, collapses 3+ blank lines to one, trims
edges).

Implemented next (section 6): a format-aware structural cleaning stage,
`document_cleaning.clean_document`, that strips PDF line-number gutters and
running headers/footers and merges hyphenated line breaks, guarded so it can
never gut an unusual layout. `normalize_extracted_text` now only does the final
whitespace tidy.

The single biggest artifact win is upstream: selecting HTML/XML over PDF (section
1) avoids the noisy PDF text layer entirely for bills that offer HTML.

### Hints from LOCUS and OpenStates for more aggressive cleanup

- OpenStates scrapers (and this repo's `openstates_parser.py`) show the pattern
  worth copying: per-jurisdiction handling rather than one global regex. Our
  `text_normalization.py` already does this for HTML strikeouts; the same
  per-state approach is the right model for PDF gutter/header/footer stripping
  when we build that opt-in pass. OpenStates' `openstates-scrapers` repo has
  state-specific text-extraction code that is a good reference for which states
  need which cleanup.
- LOCUS builds its corpus by OCR-ing heterogeneous documents into text, then
  chunking and labeling each chunk by legal function (Context / Rules / Process /
  Enforcement) with a ModernBERT classifier. The transferable idea for us is
  structural: segment bill text into typed chunks and drop or down-weight
  non-substantive boilerplate (enacting clauses, captions, page furniture)
  instead of trying to regex it away. That is a cleaner path to "remove artifact
  outputs" than line-level heuristics, and it doubles as useful structure for
  downstream analysis.

## 4. LOCUS: what it is and why it is relevant

Paper: "Freeing the Law with LOCUS: A Local Ordinance Corpus for the United
States" (arXiv 2606.19334, Peskoff, Barrow, Vu, Davenport, May 2026).

Resources:
- Paper: https://arxiv.org/abs/2606.19334
- Dataset: https://huggingface.co/datasets/LocalLaws/LOCUS-v1
- Models (ModernBERT classifiers + scorers) and dataset org: https://huggingface.co/LocalLaws
- TrueSkill dimension leaderboard: https://locallaws--locus-leaderboards-web.modal.run
- Third-party platform built on LOCUS (not the authors'): https://github.com/evcatalyst/evolocus

Note: the authors did not release a processing-pipeline GitHub repo; the paper
distributes data and the trained models through the Hugging Face `LocalLaws` org
(above), so the pipeline is described in paper section 4 rather than published as
code. The only GitHub is the third-party EvoLOCUS platform.

- Scope: U.S. **municipal and county ordinances** (local law), not state/federal
  bills. Raw corpus from 9,239 cities/counties; a harmonized layer covers 2,309
  of 3,144 counties. LOCUS-v1 is ~2.2M ordinances, ~1.77 GB, parquet, chunk-level.
- Labels: each text chunk gets a `function` (Context / Rules / Process /
  Enforcement), a binary `substantive` indicator, and, for substantive chunks, a
  `topic` (Buildings / Business / Nuisance / Zoning / Other). Built with
  ModernBERT classifiers trained for the paper.
- License: dataset is CC BY-NC 4.0 (non-commercial); paper/models CC BY 4.0.
  Note the non-commercial clause before any product use.
- Caveats (from the dataset card): not a complete census of U.S. local law and
  not a fully human-validated benchmark without additional auditing.

Relevance to CNTR:
- Different layer of government (local ordinances vs our AI-focused state/federal
  bills), so it is not a drop-in data source for our LegiScan pipeline and does
  not track bill versions/amendments. Overlap with our AI-bill scope is likely
  small.
- The valuable transfer is **the processing pipeline**, detailed next.
- Possible future use: if CNTR ever extends to local AI ordinances, LOCUS is the
  reference corpus (mind the NC license).

### 4a. How LOCUS processes documents (paper section 4), and what we can learn

Their pipeline, stage by stage (arXiv 2606.19334 sections 4-5):

1. **Collection (4.1).** Not one generic scraper: browser automation plus
   *vendor-specific* download logic per hosting platform (Municode, American
   Legal Publishing). They enumerate the failure modes they hit (server-side PDF
   assembly limits, filename collisions from non-unique municipality names,
   hidden interface thresholds, 15s crawl delays, anti-bot, consolidated
   multi-county cities) and fall back to manual collection for restricted codes.
   Raw corpus: 9,239 PDFs, ~80 GB, ~7M pages.
2. **Normalize everything to Markdown via VLM OCR (4.3).** They run OCR on every
   page (born-digital, exported, and scanned alike) with LightOnOCR-2-1B, an open
   1B-param vision-language model (Qwen-3 based, finetuned on 16M PDF pages,
   strong on OlmOCR-Bench). It emits Markdown in correct reading order and handles
   single- and double-column and scanned layouts. Run on Modal batch inference at
   ~$0.30 per 1,000 pages. Format diversity collapses into one representation
   before any cleaning.
3. **Post-processing on the unified Markdown (4.3).** Strip repeated headers,
   footers, and page numbers; merge content that crosses pages (paragraphs and
   tables); then segment into individual laws by detecting section/subsection
   headers. That is the artifact-removal and structure step our PDF path lacks.
4. **Structural-vs-substantive filtering (4.2).** A two-level zero-shot LLM pass
   (GPT-5.4-nano, chosen for cost after comparing larger models) removes stray
   headers / tables of contents and flags substantive chunks. Hardest 5.5% are
   re-judged by a bigger model (LLM-as-judge); it agreed on 64,977/108,889.
   Purely structural segments are dropped from the release.
5. **Distill LLM labels into cheap ModernBERT classifiers (4.4).** Sample 100k
   laws, label with GPT-5.4-nano, then train a ~100M-param ModernBERT-base
   classifier (80k train / 10k sweep / 10k eval) for substantivity, function, and
   topic, so inference runs cheaply over the whole corpus.
6. **Dimensional scorers (5.1).** Per dimension (enforcement discretion, opacity,
   paternalism), 200k pairwise LLM-as-judge matchups (both orders, to cancel
   position bias) -> TrueSkill latent scores -> z-normalize -> ModernBERT
   regression head (MSE loss), Pearson 0.82-0.94 on held-out.

**Lessons for our LegiScan pipeline (do / don't):**

- **Do adopt "one normalized representation, then one shared cleaning pass."**
  Today we branch per MIME with uneven cleanup. Converging all formats to
  Markdown and running a single post-processing stage (strip headers/footers/page
  numbers, merge cross-page paragraphs, segment by section header) is the biggest
  win available and answers "improve removal of artifact outputs." The Utah file
  `md_all/UT_SB0038_PDF_2060739.md` shows what that pass fixes: line-number
  gutters, a page-number "2" on its own line, and a heading glued to a stray "1".
- **Do add section segmentation.** Detecting section/subsection headers and
  splitting bill text into structured units (rather than one blob) is what makes
  version diffing and downstream analysis tractable. LOCUS treats the *section*
  as the unit; we should too.
- **Do consider a VLM-OCR fallback for PDFs.** For bills where no HTML/XML exists
  (or the PDF is scanned/double-column and our PyMuPDF text layer mangles it),
  LightOnOCR-2-1B style OCR-to-Markdown is far cleaner than `pymupdf4llm`, cheap,
  and batchable. This upgrades `_extract_pdf`'s fallback specifically for the hard
  cases; it is not needed for HTML-available bills.
- **Do reuse the "LLM-annotate a sample, distill to ModernBERT" pattern** for any
  at-scale labeling we want (e.g. confirming a bill is genuinely AI-related, or
  our stakeholder-perspective tags), instead of calling an LLM per bill in the
  analysis service.
- **Do NOT copy "OCR everything."** LOCUS OCRs all pages because local ordinances
  are often scanned and rarely offer HTML. Our LegiScan bills are usually
  born-digital with HTML/XML available, so OCR-by-default would be slower and
  lossier than using the structured source. Our HTML/XML-first selection (section
  1) is the right default; VLM-OCR is a targeted fallback, not the front door.
- **Their format rule matches the one in section 1.** LOCUS's decision tree
  "prioritizes HTML due to structural predictability," which is the same call we
  made independently.
- **Provenance caveat to carry over.** They are explicit that LLM-as-judge labels
  are not a substitute for lawyer/judge review. Any classifier we distill should
  keep the same disclaimer (consistent with our non-overclaiming stance).

**Concrete next steps this suggested:**
1. Shared Markdown post-processing pass (gutter/header/footer strip) - **done**,
   see section 6 (`document_cleaning`).
2. Section-header segmentation - **done**, see section 6 (`section_segmentation`).
3. VLM-OCR fallback for scanned/double-column PDF bills - **still open**; prototype
   against the current PyMuPDF output on a handful of known-bad PDFs.

## 5. Bill text versioning

Decision: track multiple text versions per bill (Introduced / Engrossed /
Enrolled / ...), not only the latest.

Implemented: `enumerate_text_versions(entries)` returns one best-format document
per version, newest first, using the same HTML/XML-first ranking.
`select_latest_text_entry` stays the "latest only" shortcut and equals
`enumerate_text_versions(...)[0]`. This is the reusable primitive for either
storing every version or letting users pick a version.

Follow-up (needs the schema owner): the `bills` table currently holds a single
`full_text`. Persisting multiple versions needs a schema change (e.g. a
`bill_text_versions` table keyed by bill with `doc_id`, `date`, `type`,
`mime_id`, `text`), which lives in Supabase outside this repo, plus repository
and ingestion wiring to fetch and upsert each enumerated version. Not landed here
because there is no migration surface in the repo to review.

## 6. Implemented cleaning + segmentation pipeline

Combines the deterministic strategies from LOCUS and the legislative-NLP
literature into one stage, keeping our per-state HTML handling.

**Stage order** (`text_extraction.extract_text_from_mime`):

1. MIME-specific extraction to markdown (`_extract_html` runs the per-state
   `text_normalization` on the HTML soup first, unchanged).
2. `document_cleaning.clean_document(text, mime_id)` - the shared, format-aware
   structural pass.
3. `normalize_extracted_text` - final whitespace tidy.

Segmentation (`section_segmentation.segment_sections`) is a non-destructive,
metadata-only step callers can run on the cleaned text; it is re-exported from
`text_extraction`.

**`document_cleaning` (new).** Applies only to PDF-family formats (mime 2/3/4/5);
HTML/XML pass through with just de-hyphenation, so per-state normalization is
never touched.
- `strip_line_number_gutters`: removes a per-page *leading* line-number column,
  but only after verifying the leading integers actually behave like a monotonic,
  page-resetting gutter (so coincidental numbered lines survive).
- `strip_inline_gutters`: removes an *in-line* line-number column (CO, LA, MD, and
  some UT/FL bills render the counter mid-paragraph: "...EDUCATION. 15 7-2201.
  16 IN THIS SUBTITLE 17 ..."). A numeric token is only removed when a value one
  above or below it sits within a few tokens (i.e. it is part of a running
  sequence); prose numbers (years, district ordinals, statute parts) have no
  sequential neighbour and are kept. This is what finally cleans Colorado, which
  the frequency guard alone could not.
- `strip_page_markers`: drops bare page numbers, `Page N of M` footers (also
  `Page of M` after the inline pass), and short dash-wrapped footers. These are
  pattern-verified furniture (never statutory text), so they run **unguarded** and
  clean even short bills where the frequency rule trips the safety guard.
- `remove_repeated_furniture`: drops running headers/footers by exact-repeat
  frequency. Matching is exact (not digit-masked) so appropriations line items like
  `$153,663,700 from General Fund` are preserved; `(a)`-style markers are
  protected. Guarded (a short bill can legitimately repeat lines).
- `rejoin_split_ordinals`: rejoins superscript ordinals markdownify splits
  (`76 [th]` -> `76th`), seen across GA, KS, MA, NC and others.
- `dehyphenate`: rejoins words split across a line break.
- Global safety guard: if a *heuristic* step (gutters, frequency furniture, a state
  cleaner) would delete more than 40% of the non-whitespace characters, that step
  is reverted. Pattern-verified passes (page markers) are exempt because they
  cannot misfire.
- Validated on all ~2,187 `md_all/*_PDF_*.md` fixtures: median ~6.5% reduction,
  p99 ~22%, max ~34%, **zero catastrophic (>45%) removals**, and no state left with
  within-file residual furniture.

**Per-state cleaning registry.** `_STATE_CLEANERS` maps a 2-letter state code to a
`(text) -> text` cleaner, mirroring the per-jurisdiction pattern already used in
`text_normalization` for HTML strikeouts. `clean_document(text, mime_id, state)`
threads the state through (from `extract_text_from_mime`). Most artifacts turned
out to be cross-state markdown/PDF quirks best handled by the generic passes above;
the registry is reserved for genuinely state-specific formatting.
- `FL`: removes the running bill-id header (`CS/HB 693 2026`) and the draft
  doc-code footer (`hb693-01-c1`) by shape. These repeat too few times to trip the
  frequency rule on short FL bills, but never occur in statutory text.

**Diagnostic method (how the per-state work was scoped).** Rather than hand-writing
rules per state, a survey over `md_all` grouped by state measured, per state, the
cleaning reduction, guard-revert rate, inline-gutter incidence, and residual
repeated furniture. Two lessons shaped the design: (1) most "per-state" artifacts
are actually shared markdownify/PDF quirks (inline gutters, split ordinals, page
footers) better fixed once, generically but safely; (2) naive proxy metrics lie -
Virginia and Georgia looked like gutter states but their "numbers" were numbered
provisions and district ordinals (content), so the detectors are sequence- and
pattern-validated to leave prose untouched.

**`section_segmentation` (new).** Splits cleaned text into ordered sections on
start-of-line `Section N` / `Sec. N` / `SECTION N` markers (markdown-emphasis
tolerant), with a `preamble` section for leading text and a single-section
fallback when no markers exist. Numbers handle code-style ids (`1-101`, `12A`).
Joining section bodies reproduces the input (lossless).

### Techniques borrowed, and their sources

- **HTML-first, OCR-as-fallback; per-source scraping logic; section as the unit**
  - LOCUS, "Freeing the Law with LOCUS" (arXiv 2606.19334).
- **Frequency-based header/footer removal** (boilerplate recurs across pages;
  statutory text does not) - the standard approach in legislative-PDF pipelines,
  e.g. "From Parliamentary Rhetoric to Enacted Law" (arXiv 2606.00030) and the
  header/footer-as-classification framing in structural-segmentation work.
- **Structural section segmentation of legal documents** - Aumiller et al.,
  "Structural Text Segmentation of Legal Documents" (arXiv 2012.03619); statute
  hierarchy as structured objects rather than flat text - Bundesrecht (arXiv
  2605.31338) and CLaw (arXiv 2509.21208).
- **Per-jurisdiction handling over one global regex** - OpenStates scrapers and
  our own `text_normalization.py`; kept intact for HTML.

### Still open (bigger, out of this change)

- VLM-OCR fallback for scanned/double-column PDFs (LOCUS uses LightOnOCR-2-1B).
  Inline gutters (CO/LA/MD) are now removed by `strip_inline_gutters`; a
  layout-aware extractor (`pymupdf_layout`) or VLM-OCR would still improve the
  hardest double-column scans upstream.
- Distilling LLM labels into a ModernBERT substantive/function classifier to drop
  boilerplate structurally and add analysis metadata.
- Persisting sections/versions (needs the Supabase schema, see section 5).
