import redis

from utils.config import config


class RedisClient:
    def __init__(self):
        if config.redis_password:
            # Use redis.from_url with password authentication
            self.client = redis.from_url(
                config.redis_uri,
                password=config.redis_password,
                decode_responses=True
            )
        else:
            # Use URL without password
            self.client = redis.from_url(
                config.redis_uri, decode_responses=True
            )

    def set(self, key: str, expiry: int, value: str) -> bool:
        try:
            return self.client.setex(key, expiry, value)
        except Exception as e:
            print(f"Error setting key in Redis: {e}")
            return False

    def get(self, key: str) -> str | None:
        try:
            return self.client.get(key)
        except Exception as e:
            print(f"Error getting key from Redis: {e}")
            return None

    def list(self) -> list[str] | None:
        try:
            return self.client.keys()
        except Exception as e:
            print(f"Error listing key from Redis: {e}")
            return None

    def delete(self, key: str) -> bool:
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            print(f"Error deleting key from Redis: {e}")
            return False


redis_client = RedisClient()
