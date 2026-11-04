import logging
import os
import time
from concurrent.futures import Future
from queue import Queue
from threading import Lock, Thread
from uuid import uuid4

from google.protobuf.message import DecodeError
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry
from redis.sentinel import Sentinel


log = logging.getLogger(__name__)

def flight_key(flight_id):
    return f"flight:{flight_id}"


def search_key(origin, destination, day):
    return f"search:{origin}:{destination}:{day or 'all'}"


class FlightCache:
    def __init__(self, client, ttl):
        self._client = client
        self._ttl = ttl
        self._lock = Lock()
        # A single Flight Service process owns the cache. A new process uses a
        # different token even if Redis still holds data from its predecessor.
        self._initial_token = uuid4().bytes
        self._tokens = {}
        self._retry_at = 0.0
        self._job = None
        self._queue = Queue(maxsize=1)
        # A DNS lookup can block beyond redis-py's socket timeout. Keep that
        # work off gRPC threads, and let process shutdown discard a stuck lookup.
        Thread(target=self._work, name="redis-cache", daemon=True).start()

    @classmethod
    def from_env(cls):
        ttl = int(os.getenv("CACHE_TTL_SECONDS", "420"))
        if not 300 <= ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 300 and 600")
        address = (
            os.getenv("REDIS_SENTINEL_HOST", "redis-sentinel"),
            int(os.getenv("REDIS_SENTINEL_PORT", "26379")),
        )
        transport = {
            "socket_connect_timeout": 0.25,
            "socket_timeout": 0.25,
            "retry": Retry(NoBackoff(), 0),
        }
        # Discovery may wait for Sentinel's DNS lookup during failover. The
        # Redis worker can finish that reconnect after the RPC falls back to SQL.
        discovery = transport | {"socket_timeout": 1.5}
        sentinel = Sentinel([address], sentinel_kwargs=discovery, **transport)
        return cls(sentinel.master_for(os.getenv("REDIS_MASTER_NAME", "flight-redis")), ttl)

    def _failed(self, error):
        log.warning("Redis unavailable, bypassing cache: %s", error)
        self._retry_at = time.monotonic() + 1.0

    def _work(self):
        while True:
            job, command, args, kwargs = self._queue.get()
            try:
                job.set_result(command(*args, **kwargs))
            except Exception as error:
                job.set_exception(error)

    def _redis(self, command, *args, **kwargs):
        # Called with the cache lock. An overdue operation keeps its slot
        # until it finishes, so calls during an outage cannot pile up.
        if time.monotonic() < self._retry_at or (self._job and not self._job.done()):
            return None
        self._job = Future()
        self._queue.put_nowait((self._job, command, args, kwargs))
        try:
            return self._job.result(timeout=0.18)
        except TimeoutError:
            self._failed("cache operation exceeded 180 ms")
        except RedisError as error:
            self._failed(error)
        return None

    def invalidate_flight(self, flight):
        keys = (
            flight_key(flight["id"]),
            search_key(flight["origin"], flight["destination"], flight["departure_day"]),
            search_key(flight["origin"], flight["destination"], None),
        )
        with self._lock:
            # Reject an old value even if DEL fails or a promoted replica still
            # has the value that preceded the database change.
            token = uuid4().bytes
            for key in keys:
                self._tokens[key] = token
            self._redis(self._client.delete, *keys)
            log.info("cache invalidate keys=%s", keys)

    def read(self, key, response_type, load):
        with self._lock:
            token = self._tokens.get(key, self._initial_token)
            payload = self._redis(self._client.get, key)

        if payload is not None and payload.startswith(token):
            try:
                response = response_type.FromString(payload[len(token):])
            except DecodeError:
                log.warning("invalid cache payload key=%s", key)
            else:
                log.info("cache hit key=%s", key)
                return response

        log.info("cache miss key=%s", key)
        response = load()
        with self._lock:
            # A read that started before a mutation must not restore its old value.
            current_token = self._tokens.get(key, self._initial_token)
            if token == current_token:
                self._redis(self._client.set, key, token + response.SerializeToString(), ex=self._ttl)
        return response
