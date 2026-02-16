"""
Integration tests for LegiScanClient - requires real API key and makes actual API calls.

Skipped unless LEGISCAN_API_KEY is set and --integration is passed.
Run: pytest tests/services/ingestion/test_legiscan_client_integration.py --integration
"""
import os

import pytest
from datetime import date

from services.ingestion.src.legiscan_client import LegiScanClient
from services.ingestion.src.parser import parse_bill_data
from shared.models.bill import Bill, BillBody


@pytest.fixture
def client(request):
    """Create a real LegiScanClient instance"""
    if not request.config.getoption("--integration", default=False):
        pytest.skip("Integration tests require --integration flag")
    api_key = os.getenv("LEGISCAN_API_KEY")
    if not api_key:
        pytest.skip("LEGISCAN_API_KEY environment variable not set")
    return LegiScanClient(api_key=api_key)


class TestLegiScanClientIntegration:
    """Integration tests for LegiScanClient"""
    
    def test_client_initialization(self, client):
        """Test that client initializes with real API key"""
        assert client.api_key is not None
        assert client.base_url == "https://api.legiscan.com"
        print(f"Client initialized with API key (hidden)")
    
    def test_search_ai_bills_real(self, client):
        """Test searching for AI-related bills with real API"""
        print("Searching for AI-related bills with relevance > 0...")
        results, summary = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        assert len(results) > 0
        assert all(result["relevance"] > 0 for result in results)
        assert summary["count"] > 0
        assert summary["relevancy"] == "0-100"
    
    def test_get_bill_real(self, client):
        """Test fetching a real bill from LegiScan"""
        print("Searching for a test bill with relevance > 0...")
        results, _ = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        if not results:
            pytest.skip("No bills found in search to test get_bill")
        
        test_bill_id = results[0]["bill_id"]
        print(f"Fetching bill {test_bill_id}...")
        
        bill_data = client.get_bill(test_bill_id)
        
        assert isinstance(bill_data, dict)
        assert bill_data.get("bill_id") == test_bill_id
        assert "title" in bill_data
        assert "state" in bill_data
        
        print(f"Retrieved bill: {bill_data.get('title', 'N/A')[:50]}...")
        print(f"  State: {bill_data.get('state')}")
        print(f"  Bill Number: {bill_data.get('bill_number')}")
    
    def test_parse_bill_data_real(self, client):
        """Test parsing a real bill from LegiScan"""
        print("Getting a real bill to parse...")
        results, _ = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        if not results:
            pytest.skip("No bills found in search to test parsing")
        
        test_bill_id = results[0]["bill_id"]
        bill_data = client.get_bill(test_bill_id)
        
        print(f"Parsing bill {test_bill_id}...")
        bill = parse_bill_data(bill_data)
        
        assert isinstance(bill, Bill)
        assert bill.external_id  # Composite format: "STATE BILL_NUMBER VERSION_DATE"
        assert bill.title is not None
        assert bill.state is not None
        assert bill.year is not None
        assert bill.bill_number is not None
        assert bill.body in [BillBody.HOUSE, BillBody.SENATE, BillBody.ASSEMBLY]
        
        print(f"Parsed bill successfully:")
        print(f"  External ID: {bill.external_id}")
        print(f"  Title: {bill.title[:60]}...")
        print(f"  State: {bill.state}")
        print(f"  Year: {bill.year}")
        print(f"  Bill Number: {bill.bill_number}")
        print(f"  Body: {bill.body}")
        if bill.version_date:
            print(f"  Version Date: {bill.version_date}")
        if bill.summary:
            print(f"  Summary: {bill.summary[:60]}...")
    
    def test_get_bills_from_search_results_real(self, client):
        """Test batch fetching and parsing real bills"""
        print("Testing batch bill fetching...")
        results, _ = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        if not results:
            pytest.skip("No bills found in search")
        
        # Limit to first 3 bills for testing
        test_results = results[:3]
        print(f"Fetching {len(test_results)} bills...")
        
        bills = client.get_bills_from_search_results(test_results)
        
        assert len(bills) > 0
        assert len(bills) <= len(test_results)
        assert all(isinstance(bill, Bill) for bill in bills)
        
        print(f"Successfully fetched and parsed {len(bills)} bills")
        for i, bill in enumerate(bills, 1):
            print(f"  {i}. {bill.bill_number} ({bill.state}) - {bill.title[:40]}...")
    
    def test_rate_limiting_real(self, client):
        """Test that rate limiting works with real API"""
        print("Testing rate limiting...")
        results, _ = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        if len(results) < 2:
            pytest.skip("Need at least 2 bills to test rate limiting")
        
        import time
        start_time = time.time()
        
        bill1 = client.get_bill(results[0]["bill_id"])
        bill2 = client.get_bill(results[1]["bill_id"])
        
        elapsed = time.time() - start_time
        
        assert bill1 is not None
        assert bill2 is not None
        # Should take at least 100ms (rate limit delay) between requests
        assert elapsed >= 0.1, f"Rate limiting not working (took {elapsed}s)"
        
        print(f"Rate limiting working (took {elapsed:.2f}s for 2 requests)")
    
    def test_full_workflow_bills_after_date(self, client):
        """Test full workflow: retrieve all AI-related bills after 2025-07-02"""
        cutoff_date = date(2025, 7, 2)
        print(f"\n{'='*60}")
        print(f"Full Workflow Test: AI-related bills after {cutoff_date}")
        print(f"{'='*60}")
        
        # Step 1: Search for all AI-related bills
        print("\nStep 1: Searching for AI-related bills (relevance > 0)...")
        search_results, summary = client.search_ai_bills(min_relevance=0, state="ALL", use_raw=True)
        
        assert len(search_results) > 0, "Should find at least some AI-related bills"
        assert summary["count"] > 0, "Search summary should show results"
        print(f"Found {summary['count']} total results, {len(search_results)} with relevance > 0")
        
        # Step 2: Fetch detailed bill data
        print(f"\nStep 2: Fetching detailed metadata for {len(search_results)} bills...")
        print("  Note: This may take several minutes due to rate limiting (100ms per request)")
        print(f"  Estimated time: ~{len(search_results) * 0.1 / 60:.1f} minutes")
        bills = client.get_bills_from_search_results(search_results)
        
        assert len(bills) > 0, "Should successfully fetch at least some bills"
        print(f"Successfully fetched {len(bills)} bills")
        
        # Step 3: Filter bills after cutoff date
        print(f"\nStep 3: Filtering bills with version_date after {cutoff_date}...")
        filtered_bills = [
            bill for bill in bills 
            if bill.version_date and bill.version_date > cutoff_date
        ]
        
        bills_without_date = [bill for bill in bills if not bill.version_date]
        bills_before_date = [
            bill for bill in bills 
            if bill.version_date and bill.version_date <= cutoff_date
        ]
        
        print(f"Filtered results:")
        print(f"  - Bills after {cutoff_date}: {len(filtered_bills)}")
        print(f"  - Bills on/before {cutoff_date}: {len(bills_before_date)}")
        print(f"  - Bills without version_date: {len(bills_without_date)}")
        print(f"  - Total bills fetched: {len(bills)}")
        
        # Step 4: Verify all filtered bills meet criteria
        print(f"\nStep 4: Verifying filtered bills...")
        for bill in filtered_bills:
            assert bill.version_date > cutoff_date, \
                f"Bill {bill.external_id} has version_date {bill.version_date} not after {cutoff_date}"
            assert bill.external_id, "Bill should have external_id"
            assert bill.title, "Bill should have title"
            assert bill.state, "Bill should have state"
            assert bill.year, "Bill should have year"
            assert bill.bill_number, "Bill should have bill_number"
            assert bill.body in [BillBody.HOUSE, BillBody.SENATE, BillBody.ASSEMBLY], \
                f"Bill {bill.external_id} should have valid body type"
        
        print(f"All {len(filtered_bills)} filtered bills are valid")
        
        # Step 5: Display sample results
        if filtered_bills:
            print(f"\nStep 5: Sample bills after {cutoff_date}:")
            for i, bill in enumerate(filtered_bills[:10], 1):
                print(f"  {i}. {bill.bill_number} ({bill.state}) - {bill.title[:50]}...")
                print(f"     Version Date: {bill.version_date}, Body: {bill.body}")
        else:
            print(f"\nNo bills found with version_date after {cutoff_date}")
            print("   This might be expected if no bills were updated after that date.")
            print("   Note: Bills without version_date are excluded from this filter.")
        
        # Summary
        print(f"\n{'='*60}")
        print("Full Workflow Test Summary:")
        print(f"  Total search results: {len(search_results)}")
        print(f"  Bills successfully fetched: {len(bills)}")
        print(f"  Bills after {cutoff_date}: {len(filtered_bills)}")
        print(f"  Bills on/before {cutoff_date}: {len(bills_before_date)}")
        print(f"  Bills without version_date: {len(bills_without_date)}")
        print(f"{'='*60}")
        
        # Assertions for test validity
        assert len(bills) > 0, "Should fetch at least some bills"
        # Note: filtered_bills might be 0 if no bills were updated after the date
