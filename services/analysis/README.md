# Analysis Service

Computes bill-to-bill text similarity to surface **model bills** and derivative
copies across the corpus.

## What it does

1. Load every bill in Supabase that has `full_text` (populated by the ingestion
   service's `--full-text` path).
2. Score each unique pair of bills for text overlap.
3. Classify and store the relationships that clear a threshold.

## Method

Deterministic and dependency-free (Python stdlib only): the score is the mean of

- **token-set Jaccard**: shared vocabulary, and
- **k-gram shingle Jaccard** (default k=3): shared phrasing, which catches
  verbatim copied passages a bag-of-words misses.

Scores bucket into `classification`:

| Score | classification |
| --- | --- |
| ≥ 0.60 | `model_or_derivative` (near-duplicate: model bill / copy) |
| ≥ 0.30 | `related` (substantial shared language) |
| < 0.30 | `unrelated` (not recorded) |

Bill text is formulaic, so thresholds are tuned to flag genuine copying over the
shared boilerplate every bill contains. The scorer can be swapped for an
embedding-based one later without changing the service shape.

## Usage

Run from the repo root:

```bash
python -m services.analysis.src                     # compare all bills with full_text
python -m services.analysis.src --min-score 0.4      # stricter threshold
python -m services.analysis.src --limit 200          # first 200 bills (testing)
python -m services.analysis.src --dry-run            # compute but do not write
```

Prints the number of similarity relationships stored (or that would be stored).

## Database

Requires a `bill_similarities` table keyed on the pair of bill `external_id`s:

```sql
create table if not exists bill_similarities (
    id uuid primary key default gen_random_uuid(),
    source_bill_id text not null,
    target_bill_id text not null,
    score          double precision not null,
    classification text not null,
    method         text not null,
    created_at     timestamptz default now(),
    unique (source_bill_id, target_bill_id)
);
```

## Environment Variables

Required in `.env`:

```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```
