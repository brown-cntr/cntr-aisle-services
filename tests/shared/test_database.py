from shared.database.supabase_client import get_supabase_client

def test_supabase_connection():
    client = get_supabase_client()
    data = client.table("bills").select("*").limit(1).execute().data
    assert client is not None
    assert len(data) > 0

if __name__ == "__main__":
    test_supabase_connection()
    print("✓ Test passed!")