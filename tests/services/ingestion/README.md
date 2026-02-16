# Ingestion Service Tests

Unit and integration tests for the ingestion service. `conftest.py` adds the project root to `sys.path` and disables logging during tests.

## Layout

| File | What it tests |
|------|----------------|
| `test_parser.py` | `parse_bill_data`, `_map_chamber_to_body` (parser module) |
| `test_legiscan_client.py` | `LegiScanClient` (API client, search, get_bill, get_bills_from_search_results) |
| `test_bills_repository.py` | `BillsRepository` (store_bills, get_existing_*, get_last_run_timestamp) |
| `test_filtering.py` | `filter_bills_for_processing` |
| `test_ingestion.py` | `IngestionService.ingest_ai_bills` (orchestration only) |
| `test_ingestion_integration.py` | IngestionService + real Supabase (requires `--integration`) |
| `test_ingestion_full_integration.py` | Full E2E: LegiScan + Supabase (requires `--integration`) |
| `test_legiscan_client_integration.py` | LegiScanClient + real API (requires `--integration`) |

## Running tests

```bash
# All unit tests (no integration)
pytest tests/services/ingestion/ -v

# Exclude integration-only files
pytest tests/services/ingestion/ -v --ignore=tests/services/ingestion/test_ingestion_integration.py --ignore=tests/services/ingestion/test_ingestion_full_integration.py --ignore=tests/services/ingestion/test_legiscan_client_integration.py

# Integration tests (need env: LEGISCAN_API_KEY, SUPABASE_URL, SUPABASE_KEY)
pytest tests/services/ingestion/ --integration -v

# Single file
pytest tests/services/ingestion/test_parser.py -v
pytest tests/services/ingestion/test_legiscan_client.py -v
```

## Integration tests

- Require `--integration` and the right env vars.
- `test_legiscan_client_integration.py`: real LegiScan API only.
- `test_ingestion_integration.py`: real Supabase only.
- `test_ingestion_full_integration.py`: real LegiScan + Supabase; inserts real rows (not cleaned up).
