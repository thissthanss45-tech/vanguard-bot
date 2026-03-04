import json
import pickle
import time
from typing import Any


PICKLE_PREFIX = b"__pickle__:"


class BaseCache:
    def get(self, namespace: str, key: str):
        raise NotImplementedError

    def set(self, namespace: str, key: str, value: Any, ttl_sec: int):
        raise NotImplementedError


class MemoryCache(BaseCache):
    def __init__(self):
        self._data: dict[str, tuple[float, Any]] = {}

    def get(self, namespace: str, key: str):
        token = f"{namespace}:{key}"
        item = self._data.get(token)
        if not item:
            return None
        expires_at, value = item
        if expires_at < time.time():
            self._data.pop(token, None)
            return None
        return value

    def set(self, namespace: str, key: str, value: Any, ttl_sec: int):
        token = f"{namespace}:{key}"
        self._data[token] = (time.time() + ttl_sec, value)


class DiskCacheBackend(BaseCache):
    def __init__(self, directory: str):
        from diskcache import Cache

        self.cache = Cache(directory)

    def get(self, namespace: str, key: str):
        return self.cache.get(f"{namespace}:{key}")

    def set(self, namespace: str, key: str, value: Any, ttl_sec: int):
        self.cache.set(f"{namespace}:{key}", value, expire=ttl_sec)


class RedisCacheBackend(BaseCache):
    def __init__(self, redis_url: str):
        import redis

        self.client = redis.Redis.from_url(redis_url)

    def get(self, namespace: str, key: str):
        raw = self.client.get(f"{namespace}:{key}")
        if raw is None:
            return None

        if isinstance(raw, (bytes, bytearray)) and raw.startswith(PICKLE_PREFIX):
            try:
                return pickle.loads(raw[len(PICKLE_PREFIX):])
            except Exception:
                return None

        try:
            return json.loads(raw)
        except Exception:
            return None

    def set(self, namespace: str, key: str, value: Any, ttl_sec: int):
        cache_key = f"{namespace}:{key}"
        try:
            payload = json.dumps(value, ensure_ascii=False)
            self.client.setex(cache_key, ttl_sec, payload)
            return
        except Exception:
            pass

        try:
            payload = PICKLE_PREFIX + pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            self.client.setex(cache_key, ttl_sec, payload)
        except Exception:
            # Если сериализация не удалась, лучше пропустить кэш, чем ломать основной поток.
            return


def build_cache(backend: str, cache_dir: str, redis_url: str) -> BaseCache:
    mode = (backend or "").lower()
    if mode == "redis" and redis_url:
        try:
            return RedisCacheBackend(redis_url)
        except Exception:
            return MemoryCache()
    if mode == "diskcache":
        try:
            return DiskCacheBackend(cache_dir)
        except Exception:
            return MemoryCache()
    return MemoryCache()
