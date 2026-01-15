"""A tiny Distributed Shared Memory layer on top of Redis.

- Provides read and write with per key versioning.
- Uses optimistic concurrency control: write requires expected version.
- Consistency model: sequential consistency per key for successful writers.

This is intentionally small so you can explain it in a presentation.
"""

import json
from dataclasses import dataclass
from typing import Any, Optional, Tuple
import redis

@dataclass
class DSMValue:
    value: Any
    version: int

class DSM:
    def __init__(self, redis_url: str, namespace: str):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.ns = namespace

    def _k(self, key: str) -> str:
        return f"dsm:{self.ns}:{key}"

    def read(self, key: str) -> Optional[DSMValue]:
        raw = self.r.get(self._k(key))
        if raw is None:
            return None
        obj = json.loads(raw)
        return DSMValue(value=obj["value"], version=int(obj["version"]))

    def write(self, key: str, value: Any, expected_version: Optional[int]) -> Tuple[bool, int]:
        """Write value if expected_version matches current version.

        If expected_version is None, only succeeds if key does not exist.
        Returns (ok, new_version).
        """
        k = self._k(key)
        pipe = self.r.pipeline()
        while True:
            try:
                pipe.watch(k)
                current = pipe.get(k)
                if current is None:
                    cur_version = None
                else:
                    cur_version = int(json.loads(current)["version"])

                if expected_version is None and cur_version is not None:
                    pipe.unwatch()
                    return False, cur_version
                if expected_version is not None and cur_version != expected_version:
                    pipe.unwatch()
                    return False, cur_version if cur_version is not None else -1

                new_version = 1 if cur_version is None else cur_version + 1
                payload = json.dumps({"value": value, "version": new_version}, ensure_ascii=False)
                pipe.multi()
                pipe.set(k, payload)
                pipe.execute()
                return True, new_version
            except redis.WatchError:
                continue
            finally:
                try:
                    pipe.reset()
                except Exception:
                    pass
