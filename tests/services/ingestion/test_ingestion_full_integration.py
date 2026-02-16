"""
Full end-to-end integration tests for IngestionService:
- Real LegiScan API (LEGISCAN_API_KEY required)
- Real Supabase database (SUPABASE_URL, SUPABASE_KEY required) for read-only checks

Run: pytest tests/services/ingestion/test_ingestion_full_integration.py --integration -v

These tests do NOT insert rows. They run the pipeline (search, fetch), log the rows
that would be added, and verify the workflow and existing DB state.
"""
import os
from datetime import datetime, date

import pytest

from services.ingestion.src.ingestion import IngestionService


@pytest.fixture
def full_ingestion_service(request) -> IngestionService:
    """
    Real IngestionService wired to real Supabase + LegiScan.

    Skips unless:
    - --integration flag is provided
    - SUPABASE_URL and SUPABASE_KEY are set
    - LEGISCAN_API_KEY is set (via env or settings)
    """
    if not request.config.getoption("--integration", default=False):
        pytest.skip("Integration tests require --integration flag")

    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
        pytest.skip("SUPABASE_URL and SUPABASE_KEY environment variables must be set")

    if not os.getenv("LEGISCAN_API_KEY"):
        # LegiScanClient inside IngestionService will also raise, but we prefer a clear skip.
        pytest.skip("LEGISCAN_API_KEY environment variable must be set")

    try:
        return IngestionService()
    except Exception as e:
        pytest.skip(f"Could not initialize IngestionService: {e}")


class TestIngestionFullIntegration:
    """
    End-to-end tests: real LegiScan API + read-only Supabase checks.

    Does NOT insert rows. Runs search/fetch and logs what would be stored.
    """

    def test_ingest_ai_bills_real_legiscan_to_supabase(self, full_ingestion_service: IngestionService):
        """
        Fetch bills from LegiScan and log the rows that would be added.
        No database writes; verifies pipeline and existing DB state.
        """
        service = full_ingestion_service

        min_relevance = 0
        state = "ALL"

        existing_before = service.bills_repository.get_existing_bills_map()
        existing_count_before = len(existing_before)

        # Step 1: Check what LegiScan search returns
        print(f"\n{'='*60}")
        print(f"Step 1: Searching LegiScan for AI bills...")
        search_results, summary = service.legiscan_client.search_ai_bills(
            min_relevance=min_relevance,
            state=state,
            use_raw=True
        )
        print(f"  Found {len(search_results)} search results (total available: {summary.get('count', 'N/A')})")
        
        if not search_results:
            pytest.skip("No bills found in LegiScan search - cannot test ingestion")

        # Step 2: Note about external_id format
        # external_id format is: "STATE BILL_NUMBER VERSION_DATE"
        # Example: "CA SB53 2025-01-07" or "MN SF1394 2023-02-08"
        # Note: bill_number already includes prefix (e.g., "SB53", "AB123")
        # This is different from bill_id (which is just a number from LegiScan)
        print(f"\n  Note: external_id format is 'STATE BILL_NUMBER VERSION_DATE'")
        print(f"  (e.g., 'CA SB53 2025-01-07'), where bill_number includes prefix.")
        print(f"  Bills will be matched by this composite external_id during storage.")

        # Limit search results for testing
        print(f"\nLimiting to first 100 search results for ingestion...")
        print(f"  (This keeps the test fast while still testing the real workflow)")
        limited_search_results = search_results[:100]
        print(f"  Using {len(limited_search_results)} search results (out of {len(search_results)} total)")
        
        # For display: fetch a small sample to show most recent bills
        print(f"\n  Fetching sample of bills to show most recent ones...")
        sample_for_display = limited_search_results[:20]  # Just for display
        sample_bills_display = service.legiscan_client.get_bills_from_search_results(sample_for_display)
        
        if sample_bills_display:
            # Sort by version_date for display
            def get_sort_key(bill):
                if bill.version_date:
                    return bill.version_date
                return date.min
            sorted_display = sorted(sample_bills_display, key=get_sort_key, reverse=True)
            
            print(f"  Most recent bills from sample (top 10 by version_date):")
            for i, bill in enumerate(sorted_display[:10], 1):
                version_str = bill.version_date.isoformat() if bill.version_date else "No date"
                print(f"    {i}. {bill.external_id} - {bill.title[:60]}{'...' if len(bill.title) > 60 else ''}")
                print(f"       State: {bill.state}, Version: {version_str}")

        # Step 3: Check a few sample bills to diagnose storage behavior
        print(f"\nStep 3: Checking sample bills to diagnose storage behavior...")
        # Fetch first 5 bills
        sample_search_results = limited_search_results[:5]
        sample_bills = service.legiscan_client.get_bills_from_search_results(sample_search_results)
        
        if sample_bills:
            print(f"  Checking {len(sample_bills)} sample bills...")
            print(f"  (external_id format: 'STATE BILL_NUMBER VERSION_DATE', e.g., 'CA SB53 2025-01-07')")
            storage_diagnostics = []
            for bill in sample_bills:
                # Check if bill exists in database using the constructed external_id
                existing_check = (
                    service.supabase.table("bills")
                    .select("external_id, version_date, updated_at")
                    .eq("external_id", bill.external_id)
                    .limit(1)
                    .execute()
                )
                
                if existing_check.data:
                    existing_bill = existing_check.data[0]
                    existing_version = existing_bill.get("version_date")
                    new_version = bill.version_date.isoformat() if bill.version_date else None
                    
                    if existing_version == new_version:
                        action = "SKIP (version_date matches)"
                    else:
                        action = f"UPDATE (version_date changed: {existing_version} -> {new_version})"
                    
                    storage_diagnostics.append({
                        "external_id": bill.external_id,
                        "title": bill.title[:50] + "..." if len(bill.title) > 50 else bill.title,
                        "action": action,
                        "exists": True,
                        "db_version": existing_version,
                        "new_version": new_version
                    })
                else:
                    storage_diagnostics.append({
                        "external_id": bill.external_id,
                        "title": bill.title[:50] + "..." if len(bill.title) > 50 else bill.title,
                        "action": "INSERT (new bill)",
                        "exists": False,
                        "db_version": None,
                        "new_version": bill.version_date.isoformat() if bill.version_date else None
                    })
            
            # Print diagnostics
            print(f"\n  Storage Diagnostics (sample of {len(storage_diagnostics)} bills):")
            inserts = sum(1 for d in storage_diagnostics if d["action"].startswith("INSERT"))
            updates = sum(1 for d in storage_diagnostics if d["action"].startswith("UPDATE"))
            skips = sum(1 for d in storage_diagnostics if d["action"].startswith("SKIP"))
            
            print(f"    Would INSERT: {inserts}")
            print(f"    Would UPDATE: {updates}")
            print(f"    Would SKIP: {skips}")
            
            if skips > 0:
                print(f"\n  Sample skipped bills (version_date matches):")
                for diag in storage_diagnostics[:3]:
                    if diag["action"].startswith("SKIP"):
                        print(f"    - external_id: {diag['external_id']}")
                        print(f"      Title: {diag['title']}")
                        print(f"      DB version_date: {diag['db_version']}")
                        print(f"      New version_date: {diag['new_version']}")
            
            if inserts > 0:
                print(f"\n  Sample new bills (would be inserted):")
                for diag in storage_diagnostics[:3]:
                    if diag["action"].startswith("INSERT"):
                        print(f"    - external_id: {diag['external_id']}")
                        print(f"      Title: {diag['title']}")
                        print(f"      version_date: {diag['new_version']}")
            
            if updates > 0:
                print(f"\n  Sample bills to update (version_date changed):")
                for diag in storage_diagnostics[:3]:
                    if diag["action"].startswith("UPDATE"):
                        print(f"    - external_id: {diag['external_id']}")
                        print(f"      Title: {diag['title']}")
                        print(f"      DB version_date: {diag['db_version']} -> New: {diag['new_version']}")

        # Step 4: Fetch full details and log rows that would be added (no DB writes)
        print(f"\nStep 4: Fetching full details and logging rows that would be added (no DB writes)...")
        start_time = datetime.now()

        print(f"\n  Fetching full details for {len(limited_search_results)} bills...")
        bills_to_store = service.legiscan_client.get_bills_from_search_results(limited_search_results)
        print(f"  Fetched {len(bills_to_store)} bills with full details")

        would_insert_count = 0
        if bills_to_store:
            print(f"\n  Rows that would be added (not inserting):")
            for i, bill in enumerate(bills_to_store, 1):
                check = (
                    service.supabase.table("bills")
                    .select("external_id, version_date")
                    .eq("external_id", bill.external_id)
                    .limit(1)
                    .execute()
                )
                if check.data:
                    continue  # Would skip (already exists)
                would_insert_count += 1
                version_str = bill.version_date.isoformat() if bill.version_date else "None"
                print(f"    {would_insert_count}. external_id={bill.external_id} state={bill.state} bill_number={bill.bill_number} version_date={version_str}")
                print(f"        title={bill.title[:70]}{'...' if len(bill.title) > 70 else ''}")
            if would_insert_count == 0:
                print(f"    (All {len(bills_to_store)} bills already exist in DB; none would be inserted.)")
            else:
                print(f"\n  Total that would be inserted: {would_insert_count} (of {len(bills_to_store)} fetched)")

        duration = (datetime.now() - start_time).total_seconds()

        # Step 5: Summary and read-only DB check
        print(f"\n{'='*60}")
        print(f"End-to-End Ingestion Test Results (no rows added):")
        print(f"  State: {state}, Min Relevance: {min_relevance}")
        print(f"  Duration: {duration:.1f}s")
        print(f"  Bills fetched: {len(bills_to_store) if bills_to_store else 0}")
        print(f"  Rows that would be inserted: {would_insert_count}")
        print(f"  Existing bills in DB: {existing_count_before}")
        print(f"{'='*60}")

        assert would_insert_count >= 0, "Would-insert count should be non-negative"

        # Verify Supabase is readable and has at least one bill (from prior runs)
        result = (
            service.supabase.table("bills")
            .select("external_id, title, state, year, bill_number, body, version_date, created_at, updated_at")
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )

        data = getattr(result, "data", []) or []
        assert data, "Expected at least one bill in Supabase database (read-only check)"
        print(f"  Sample bills in database: {len(data)} bills found (across all states)")

        # Basic sanity checks on the returned rows
        for row in data:
            assert row.get("external_id"), "Bill row should have external_id"
            assert row.get("title"), "Bill row should have title"
            assert row.get("state"), "Bill row should have state"
            assert row.get("bill_number"), "Bill row should have bill_number"
            assert isinstance(row.get("year"), int), "Bill row should have integer year"
            # body is stored as a string (e.g. 'house', 'senate', 'assembly')
            assert isinstance(row.get("body"), str), "Bill row body should be stored as string"

        # Soft assertion on duration just to catch pathological slowness
        # (this is not a hard guarantee, just a sanity bound)
        assert duration < 600, f"Ingestion took unexpectedly long: {duration:.1f}s"
