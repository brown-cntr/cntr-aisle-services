from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Supabase
    supabase_url: str
    supabase_key: str
    
    # Redis
    #redis_url: str = "redis://localhost:6379"
    
    # LegiScan
    legiscan_api_key: str
    
    # Environment
    environment: str = "development"
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore" 

@lru_cache()
def get_settings() -> Settings:
    return Settings()