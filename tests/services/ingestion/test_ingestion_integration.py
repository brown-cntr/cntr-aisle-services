"""
Integration tests for IngestionService - requires real Supabase connection.

Run: pytest tests/services/ingestion/test_ingestion_integration.py --integration
"""
import os
from datetime import date
from uuid import uuid4

import pytest

from services.ingestion.src.ingestion import IngestionService
from shared.models.bill import Bill, BillBody


class TestIngestionServiceIntegration:
    """Integration tests that require real Supabase connection"""
    
    @pytest.fixture
    def service(self, request):
        """Create real IngestionService (requires Supabase credentials)"""
        # Only check for env vars if --integration flag is set
        if request.config.getoption("--integration", default=False):
            if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
                pytest.skip("SUPABASE_URL and SUPABASE_KEY environment variables required")
            
            try:
                return IngestionService()
            except Exception as e:
                pytest.skip(f"Could not initialize IngestionService: {e}")
        else:
            pytest.skip("Integration tests require --integration flag")
    
    @pytest.fixture
    def test_bill(self):
        """Test bill with unique external_id and bill_number"""
        unique_id = uuid4().hex[:8]
        return Bill(
            external_id=f"TEST_{unique_id}",  # Unique test ID
            title="Test AI Regulation Bill - Integration Test",
            state="CA",
            year=2024,
            bill_number=f"TEST_{unique_id}",  # Also make bill_number unique to avoid constraint violations
            body=BillBody.ASSEMBLY,
            summary="This is a test bill created by integration tests",
            version_date=date(2024, 1, 15)
        )
    
    def test_store_bill_to_supabase_real(self, service, test_bill, request):
        """Test storing a new bill to real Supabase database"""
        # Skip if --integration flag not provided
        if not request.config.getoption("--integration", default=False):
            pytest.skip("Integration tests require --integration flag")
        
        pytest.importorskip("supabase")
        
        # Cleanup: delete any existing test bill (by external_id and unique key)
        service.supabase.table("bills").delete().eq("external_id", test_bill.external_id).execute()
        (
            service.supabase.table("bills")
            .delete()
            .eq("state", test_bill.state)
            .eq("year", test_bill.year)
            .eq("bill_number", test_bill.bill_number)
            .eq("body", test_bill.body)
            .execute()
        )
        
        # Store the bill
        count = service.bills_repository.store_bills([test_bill])
        assert count == 1, f"Should have stored 1 new bill, but got count={count}."
        
        # Verify bill exists in database
        result = (
            service.supabase.table("bills")
            .select("external_id, title, state, bill_number, year, body")
            .eq("external_id", test_bill.external_id)
            .execute()
        )
        
        assert len(result.data) == 1, "Bill should exist in database"
        stored_bill = result.data[0]
        
        assert stored_bill["external_id"] == test_bill.external_id
        assert stored_bill["title"] == test_bill.title
        assert stored_bill["state"] == test_bill.state
        assert stored_bill["bill_number"] == test_bill.bill_number
        assert stored_bill["year"] == test_bill.year
        # In the database, body is stored as a string (e.g. \"assembly\")
        assert stored_bill["body"] == test_bill.body
        
        # Cleanup: delete test bill by external_id and unique constraint
        service.supabase.table("bills").delete().eq("external_id", test_bill.external_id).execute()
        (
            service.supabase.table("bills")
            .delete()
            .eq("state", test_bill.state)
            .eq("year", test_bill.year)
            .eq("bill_number", test_bill.bill_number)
            .eq("body", test_bill.body)
            .execute()
        )
    
    def test_store_bill_skips_duplicate(self, service, test_bill, request):
        """Test that storing the same bill twice only inserts once"""
        if not request.config.getoption("--integration", default=False):
            pytest.skip("Integration tests require --integration flag")
        
        pytest.importorskip("supabase")
        
        # Cleanup first - delete by external_id and unique constraint
        service.supabase.table("bills").delete().eq("external_id", test_bill.external_id).execute()
        (
            service.supabase.table("bills")
            .delete()
            .eq("state", test_bill.state)
            .eq("year", test_bill.year)
            .eq("bill_number", test_bill.bill_number)
            .eq("body", test_bill.body)
            .execute()
        )
        
        # Store bill first time
        count1 = service.bills_repository.store_bills([test_bill])
        assert count1 == 1, f"First store should return 1, got {count1}."
        
        # Store same bill second time (should skip - no changes)
        count2 = service.bills_repository.store_bills([test_bill])
        assert count2 == 0, "Should skip duplicate bill with no changes"
        
        # Verify only one bill exists
        result = (
            service.supabase.table("bills")
            .select("external_id")
            .eq("external_id", test_bill.external_id)
            .execute()
        )
        assert len(result.data) == 1
        
        # Cleanup - delete by external_id and unique constraint
        service.supabase.table("bills").delete().eq("external_id", test_bill.external_id).execute()
        (
            service.supabase.table("bills")
            .delete()
            .eq("state", test_bill.state)
            .eq("year", test_bill.year)
            .eq("bill_number", test_bill.bill_number)
            .eq("body", test_bill.body)
            .execute()
        )
    
    def test_get_existing_bills_map_real(self, service, request):
        """Test getting existing bills from real Supabase"""
        if not request.config.getoption("--integration", default=False):
            pytest.skip("Integration tests require --integration flag")
        
        pytest.importorskip("supabase")
        
        result = service.bills_repository.get_existing_bills_map()
        assert isinstance(result, dict)
    
    def test_filter_bills_for_processing_real(self, service, request):
        """Test filtering bills with real database"""
        if not request.config.getoption("--integration", default=False):
            pytest.skip("Integration tests require --integration flag")

        pytest.importorskip("supabase")

        existing_by_state_number = (
            service.bills_repository.get_existing_bills_by_state_number()
        )
        search_results = [
            {"bill_id": "999999999", "relevance": 90},
            {"bill_id": "888888888", "relevance": 85},
        ]
        from services.ingestion.src.filtering import filter_bills_for_processing

        filtered = filter_bills_for_processing(
            search_results,
            existing_by_state_number,
            check_existing=False,
        )
        # getSearchRaw results (no state/bill_number) are returned as-is
        assert len(filtered) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
