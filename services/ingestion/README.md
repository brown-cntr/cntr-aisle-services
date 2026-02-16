# Ingestion Service

Automated service for ingesting AI-related bills from LegiScan into Supabase.

## Overview

The ingestion service:
1. Searches LegiScan API for AI-related bills using a comprehensive keyword query
2. Fetches detailed metadata for each matching bill
3. Parses and transforms data to match database schema
4. Stores/updates bills in the database

## Components

### `legiscan_client.py`
- **LegiScanClient**: Main client for LegiScan API interactions
  - `search_ai_bills()`: Search for AI-related bills
  - `get_bill()`: Fetch detailed bill metadata
  - `get_bill_text()`: Fetch bill text documents
  - `parse_bill_data()`: Convert LegiScan format to Bill model (delegates to `parser.py`)
  - Built-in rate limiting (100ms between requests)
  - Automatic retry on 429 rate limit errors

### `parser.py`
- **parse_bill_data()**: Converts raw LegiScan bill dict to our `Bill` model (external_id, body mapping, version_date, etc.)

### `ingestion.py`
- **IngestionService**: Orchestrates the full ingestion workflow
  - `ingest_ai_bills()`: Main entry point for ingestion
  - `_store_bills()`: Handles Supabase storage (insert/update)
  - Command-line interface for running ingestion

## Usage

### Command Line

Run from the project root (so `shared` and `services` are on the path). Use `python -m services.ingestion.src` or `python -m services.ingestion.src.ingestion`:

```bash
# Incremental ingestion (default)
python -m services.ingestion.src

# Full ingestion - processes all bills (ignores last run timestamp)
python -m services.ingestion.src --full

# Custom relevance threshold (default is 0)
python -m services.ingestion.src.ingestion --min-relevance 75

# Specific state
python -m services.ingestion.src.ingestion --state CA

# Only store bills with version_date on or after a date
python -m services.ingestion.src.ingestion --since 2026-01-27

# Dry run - search and fetch but do not write to the database
python -m services.ingestion.src.ingestion --dry-run
```

Exit code: 0 on success (prints ingested count to stdout), 1 on failure.

### Dry Run

Use `--dry-run` to run the full pipeline (search → filter by existing bills → fetch) without inserting or updating the database. Useful to verify API connectivity, see how many bills would be processed, and inspect sample bill data.

```bash
python services/ingestion/src/ingestion.py --dry-run
```

### Incremental Updates (Cron Job Mode)

By default, the service runs in **incremental mode**, optimized for daily cron jobs:

**Best Practice for Daily Cron Jobs:**
1. **Search** (1 API call): Searches for all AI-related bills - necessary because LegiScan doesn't support date filtering
2. **Filter** (in-memory): Filters search results by what we already have (by state + bill_number). Only bills that might be new or have new versions are processed
3. **Fetch** (N API calls): Only fetches detailed data for bills that passed the filter
4. **Store**: Inserts new bills into database

**Why This Approach:**
- **Most Efficient**: Only fetches bills we haven't seen before
- **API Quota Friendly**: 
  - First run: ~2000 API calls (search + fetch all)
  - Daily runs: ~1 search + ~5-50 fetch calls (only new bills)
- **Fast**: Daily runs complete in seconds/minutes instead of hours

**Optional: Check Existing Bills**
If you want to also check existing bills for updates (slower but catches all changes):
```bash
python services/ingestion/src/ingestion.py --check-existing
```

For the first run (no previous bills), it processes all matching bills.

### Programmatic Usage

```python
from services.ingestion.src.ingestion import IngestionService

service = IngestionService()
count = service.ingest_ai_bills(
    min_relevance=0,
    state="ALL",
    use_raw=True
)
print(f"Ingested {count} bills")
```

### Direct Client Usage

```python
from services.ingestion.src.legiscan_client import LegiScanClient

client = LegiScanClient()

# Search for bills
results, summary = client.search_ai_bills(min_relevance=0, state="ALL")

# Get detailed bill data
bills = client.get_bills_from_search_results(results)

# Parse individual bill
bill_data = client.get_bill(bill_id=123456)
bill = client.parse_bill_data(bill_data)
```

## Environment Variables

Required in `.env`:
```bash
LEGISCAN_API_KEY=your_api_key_here
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

## Rate Limits

- **LegiScan API Free Tier**: 20,000 queries/month

The client includes:
- 100ms delay between requests
- Automatic retry on 429 errors (waits 60 seconds)
- Request counting for monitoring

## Data Mapping

LegiScan → Supabase Schema:
- `external_id`: Composite `"STATE BILL_NUMBER VERSION_DATE"` (e.g. `CA SB53 2025-01-07`), built from LegiScan `state`, `bill_number`, and `status_date` (or first history date)
- `title` → `title`
- `state` → `state`
- `year` (from session or session_title) → `year`
- `bill_number` → `bill_number`
- `chamber/body` → `body` (enum: house, senate, assembly)
- `description` → `summary`
- `state_link` or `url` → `url`; LegiScan `url` → `legiscan_url`
- `status_date` (or first history date) → `version_date`
- `bill_id` → `legiscan_id`
