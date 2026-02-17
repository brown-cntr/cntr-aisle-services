# Ingestion Cron Job -- Runbook

## What this job does

Searches LegiScan for AI-related legislation across all US states, fetches full
bill metadata, and stores new or updated bills in Supabase. Runs daily via
GitHub Actions.

**If it stops working**, new bills will not appear in the database until the job
is fixed and re-run.

## Where it runs

| Item | Value |
|---|---|
| Platform | GitHub Actions |
| Workflow file | `.github/workflows/ingest.yml` |
| Schedule | Daily at 06:00 UTC |
| Typical runtime | Under 2 minutes (incremental) |
| Actions URL | https://github.com/brown-cntr/cntr-aisle-services/actions/workflows/ingest.yml |

## Secrets

Stored in GitHub repo Settings > Secrets and variables > Actions:

- `SUPABASE_URL`
- `SUPABASE_KEY`
- `LEGISCAN_API_KEY`

## Normal behavior

Logs should show:

1. "Starting bill ingestion process..."
2. Search results count
3. Filtering / skipping existing bills
4. Fetching full details for new bills
5. Storage summary (inserted, skipped, updated counts)

A count of 0 ingested bills is normal if no new legislation has been
introduced since the last run.

## Failure modes

### Job fails (exit code 1)

1. Open the failed run in the Actions UI.
2. Expand the "Run ingestion" step to see the stack trace.
3. Identify the category below and follow the steps.

### Missing or invalid secrets

- **Symptom**: "SUPABASE_URL environment variable must be set" or similar
  at startup.
- **Fix**: Verify all three secrets are present and correct in repo settings.

### LegiScan API errors

- **Symptom**: "LegiScan API error" or "Rate limit exceeded after max retries".
- **Likely cause**: Expired/invalid API key, LegiScan outage, or monthly quota
  (20,000 requests on free tier) exhausted.
- **Diagnose**: Run locally with `--dry-run --limit 5` to confirm.
- **Fix**: Rotate key if expired; if quota hit, wait until next month or reduce
  scope (`--state CA --limit 50`).

### Supabase errors

- **Symptom**: "Error inserting ..." or "Error storing bill ...".
- **Likely cause**: Schema mismatch, revoked key, or Supabase outage.
- **Diagnose**: Check the Supabase dashboard for the project; run
  `--dry-run` locally to isolate whether the issue is search/fetch vs storage.
- **Fix**: Correct schema or key; re-run the workflow.

## Manual operations

### Trigger a run manually

Go to the Actions UI > "Daily Ingestion" > "Run workflow" > select `main` >
click "Run workflow".

### Run locally (dry run, no DB writes)

```bash
python -m services.ingestion.src --dry-run --limit 50
```

### Run locally (real writes)

```bash
python -m services.ingestion.src
```

### Run for a specific state

```bash
python -m services.ingestion.src --state CA
```

### Backfill bills since a specific date

```bash
python -m services.ingestion.src --since 2026-01-01
```

### Full re-ingestion (reprocess all bills)

```bash
python -m services.ingestion.src --full
```

This fetches and re-stores all bills, not just new ones. Takes longer and uses
more API quota.

## Pausing and resuming the job

**To pause**: In the Actions UI, click "Daily Ingestion" > "..." menu >
"Disable workflow".

**To resume**: Same menu > "Enable workflow". The next scheduled run will
fire at the normal time.

## Useful CLI flags

| Flag | Effect |
|---|---|
| `--dry-run` | Search and fetch but do not write to DB |
| `--limit N` | Only process N bills |
| `--since DATE` | Only store bills with version_date >= DATE |
| `--state XX` | Restrict to a single state (e.g. CA, NY) |
| `--full` | Reprocess all bills (ignore incremental filtering) |
| `--check-existing` | Also check existing bills for updates |
| `--min-relevance N` | Only include bills with relevance >= N (0-100) |
