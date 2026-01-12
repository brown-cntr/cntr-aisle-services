from supabase import create_client, Client
from functools import lru_cache
from ..utils.config import get_settings

@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """Get Supabase client singleton"""
    settings = get_settings()
    return create_client(settings.supabase_url, settings.supabase_key)