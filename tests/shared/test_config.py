from shared.utils.config import get_settings

def test_settings_load():
    settings = get_settings()
    assert settings.supabase_url is not None
