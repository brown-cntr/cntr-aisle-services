# Ingestion Cron Job -- Runbook

## What this job does

A daily GitHub Actions workflow with two jobs, run in order:

1. **ingest** searches LegiScan for AI-related bills (all states, current year)
   and inserts bills that are not yet in Supabase.
2. **sync** checks every LegiScan session in the database for changed bills
   (by `change_hash`) and re-fetches them to update status, title, etc.

**If it stops working**, new bills and status updates stop appearing until the
job is fixed and re-run.

## Where it runs

| Item | Value |
|---|---|
| Platform | GitHub Actions |
| Workflow file | `.github/workflows/ingest.yml` |
| Schedule | Daily at 06:00 UTC; sync starts after ingest finishes, even if ingest failed |
| Typical runtime | A few minutes per job; ~20 minutes for `--full` or a first load (requests are spaced 0.6s apart) |
| Actions URL | https://github.com/brown-cntr/cntr-aisle-services/actions/workflows/ingest.yml |

## Secrets

Stored in GitHub repo Settings > Secrets and variables > Actions:
`SUPABASE_URL`, `SUPABASE_KEY`, `LEGISCAN_API_KEY`.

## LegiScan limits (from 2026-10-01)

- **10,000 queries/month.** Every request counts, including failed requests and `--dry-run`.
- **~2 requests/second.** The client waits 0.6s between requests.
- Each run ends with `LegiScan queries used this run: N`. For month-to-date
  usage, see the API status report at https://legiscan.com/legiscan.

| Operation | Approximate queries |
|---|---|
| Daily ingest | 1 per search page (2,000 results/page) + 1 per new bill |
| Daily sync | 1 per session in the database + 1 per changed bill |
| `--full` / `--check-existing` | ~2,000 (about 20% of the month) |

## Normal behavior

Ingest logs, in order:

1. `Found N total results across P page(s)`
2. `Bulk check: X of Y search-result bills already in database`
3. `Fetching bill ...` for new bills only
4. `Storage complete: N new, M skipped (already in DB)`

Sync logs `Checking N session(s) for changes`, a `Session X: N bills tracked,
M changed` line per session, then `Sync complete: N bill(s) updated`.

Both end with the queries-used line. Zero new or updated bills is normal on
quiet days.

## Failure modes

Open the failed run in the Actions UI and expand **Run ingestion** or
**Sync existing bills** to see the stack trace.

### Missing or invalid secrets

- **Symptom**: `ValidationError` with `Field required` for `supabase_url`,
  `supabase_key`, or `legiscan_api_key` at startup; or a `LegiScan API error`
  about the key.
- **Fix**: Check all three secrets in repo settings; rotate the LegiScan key if expired.

### LegiScan rate limit or quota

- **Symptom**: `LegiScanRateLimitError` or "Rate limit exceeded after max
  retries". The run stops; bills fetched before the limit are still stored.
- **Diagnose**: Check the API status report on the LegiScan account and the
  queries-used lines from recent runs. Don't spend more queries testing it.
- **Fix**: If the monthly quota is used up, pause the workflow until next month.
  If it's the rate limit, make sure nobody else is running the CLI with the same
  key at the same time.

### Other LegiScan errors

- **Symptom**: `LegiScan API error: ...` or `HTTP Error ...`.
- **Likely cause**: LegiScan outage or a bad key. Wait and re-run, or rotate the key.

### Supabase errors

- **Write errors** (`Error inserting ...`, `Error storing bill ...`,
  `Error updating bill ...`) are logged per bill; the job may still pass.
- **Read errors are only logged as warnings and the job still passes**:
  `Error bulk-checking legiscan_ids`, `Error fetching distinct session IDs`,
  `Error fetching change hashes`. Sync can quietly do nothing.
- **Check**: If several runs in a row show 0 new / 0 updated, or
  `No sessions found in database`, look for these warnings and check the
  Supabase dashboard (project status, key, schema).

## Manual operations

- **Trigger a run**: Actions UI > "Daily Ingestion" > "Run workflow" (runs both jobs).
- **Pause / resume**: Actions UI > "Daily Ingestion" > "..." > "Disable workflow"
  / "Enable workflow". This pauses both jobs.

Run locally from the repo root. Every command spends LegiScan queries,
including `--dry-run`.

```bash
python -m services.ingestion.src                         # daily ingest
python -m services.ingestion.src --sync                  # update changed bills
python -m services.ingestion.src --dry-run --limit 5     # smoke test, no DB writes
python -m services.ingestion.src --legiscan-id 123456    # one bill
python -m services.ingestion.src --legiscan-url "https://legiscan.com/IL/bill/SB3890/2025"
```

## CLI flags

| Flag | Effect |
|---|---|
| `--sync` | Re-fetch bills whose `change_hash` changed; updates the whole row |
| `--backfill` | One-time: fill missing `change_hash` / `legiscan_session_id` on existing bills |
| `--legiscan-id ID` / `--legiscan-url URL` | Ingest a single bill. **Writes to the DB even with `--dry-run`** |
| `--dry-run` | Search and fetch without DB writes (still spends queries) |
| `--limit N` | Only fetch N bills |
| `--state XX` | Restrict to one state (e.g. CA) |
| `--min-relevance N` | Only include bills with relevance >= N (0-100) |
| `--since DATE` | Only store fetched bills with `version_date` >= DATE. A filter, not a backfill: the search only covers the current year |
| `--full` / `--check-existing` | Re-fetch every search result (~2,000 queries). Only refreshes status, `change_hash`, and session; use `--sync` for a full refresh |
