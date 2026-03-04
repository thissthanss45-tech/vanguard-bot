import pickle

from cache_backend import PICKLE_PREFIX, RedisCacheBackend


class DummyRedisClient:
    def __init__(self):
        self.storage = {}

    def get(self, key):
        return self.storage.get(key)

    def setex(self, key, ttl, value):
        self.storage[key] = value


class NonJsonObject:
    def __init__(self, value):
        self.value = value


def test_redis_backend_json_roundtrip():
    backend = RedisCacheBackend.__new__(RedisCacheBackend)
    backend.client = DummyRedisClient()

    payload = {"a": 1, "b": [1, 2, 3]}
    backend.set("ns", "k1", payload, 30)

    assert backend.get("ns", "k1") == payload


def test_redis_backend_pickle_fallback_roundtrip():
    backend = RedisCacheBackend.__new__(RedisCacheBackend)
    backend.client = DummyRedisClient()

    payload = {"obj": NonJsonObject(42)}
    backend.set("ns", "k2", payload, 30)

    raw = backend.client.get("ns:k2")
    assert isinstance(raw, (bytes, bytearray))
    assert raw.startswith(PICKLE_PREFIX)

    restored = backend.get("ns", "k2")
    assert restored["obj"].value == 42


def test_redis_backend_invalid_pickle_returns_none():
    backend = RedisCacheBackend.__new__(RedisCacheBackend)
    backend.client = DummyRedisClient()

    backend.client.setex("ns:k3", 30, PICKLE_PREFIX + b"broken")
    assert backend.get("ns", "k3") is None
