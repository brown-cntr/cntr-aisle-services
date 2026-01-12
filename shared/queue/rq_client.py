from redis import Redis
from rq import Queue
from functools import lru_cache
from ..utils.config import get_settings

@lru_cache(maxsize=1)
def get_redis_connection() -> Redis:
    """Get Redis connection singleton"""
    settings = get_settings()
    return Redis.from_url(settings.redis_url)

def get_queue(name: str) -> Queue:
    """Get RQ queue by name"""
    redis_conn = get_redis_connection()
    return Queue(name, connection=redis_conn)