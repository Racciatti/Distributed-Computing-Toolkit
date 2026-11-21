import json
import os
import threading
from typing import Any


class SharedMemory:
    def __init__(self, filepath: str = "shared_data.txt") -> None:
        self.filepath = filepath
        self._lock = threading.Lock()

    def read_all(self) -> dict:
        with self._lock:
            return self._load()

    def write_all(self, data: dict) -> None:
        with self._lock:
            self._dump(data)

    def get(self, key: str) -> Any:
        with self._lock:
            return self._load().get(key)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            data = self._load()
            data[key] = value
            self._dump(data)

    def delete(self, key: str) -> None:
        with self._lock:
            data = self._load()
            data.pop(key, None)
            self._dump(data)

    def _load(self) -> dict:
        if not os.path.exists(self.filepath):
            return {}
        try:
            with open(self.filepath, "r", encoding="utf-8") as fh:
                content = fh.read().strip()
            if not content:
                return {}
            return json.loads(content)
        except (json.JSONDecodeError, OSError):
            return {}

    def _dump(self, data: dict) -> None:
        with open(self.filepath, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.write("\n")
