"""Tests for src/shared_memory.py — thread-safe, file-backed key/value store.

All tests use pytest's tmp_path fixture so that no test ever touches the
real shared_data.txt file in the project root.
"""

import threading

from src.shared_memory import SharedMemory


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _sm(tmp_path, filename="data.txt") -> SharedMemory:
    """Return a SharedMemory instance backed by a temp file."""
    return SharedMemory(str(tmp_path / filename))


# ---------------------------------------------------------------------------
# read_all on empty / missing files
# ---------------------------------------------------------------------------


class TestReadEmpty:
    def test_read_nonexistent_file_returns_empty_dict(self, tmp_path):
        sm = SharedMemory(str(tmp_path / "nonexistent.txt"))
        assert sm.read_all() == {}

    def test_read_empty_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "empty.txt"
        path.write_text("")
        sm = SharedMemory(str(path))
        assert sm.read_all() == {}

    def test_read_whitespace_only_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "ws.txt"
        path.write_text("   \n  ")
        sm = SharedMemory(str(path))
        assert sm.read_all() == {}

    def test_read_invalid_json_returns_empty_dict(self, tmp_path):
        path = tmp_path / "bad.txt"
        path.write_text("not-valid-json{{{")
        sm = SharedMemory(str(path))
        assert sm.read_all() == {}


# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------


class TestSetAndGet:
    def test_set_and_get_string_value(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("name", "alice")
        assert sm.get("name") == "alice"

    def test_set_and_get_integer_value(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("count", 42)
        assert sm.get("count") == 42

    def test_set_and_get_nested_value(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("obj", {"a": [1, 2, 3]})
        assert sm.get("obj") == {"a": [1, 2, 3]}

    def test_get_missing_key_returns_none(self, tmp_path):
        sm = _sm(tmp_path)
        assert sm.get("missing") is None

    def test_overwrite_existing_key(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("k", "first")
        sm.set("k", "second")
        assert sm.get("k") == "second"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDeleteKey:
    def test_delete_existing_key(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("to_remove", "value")
        sm.delete("to_remove")
        assert sm.get("to_remove") is None

    def test_delete_nonexistent_key_is_noop(self, tmp_path):
        sm = _sm(tmp_path)
        sm.delete("ghost")  # must not raise

    def test_delete_does_not_affect_other_keys(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("a", 1)
        sm.set("b", 2)
        sm.delete("a")
        assert sm.get("b") == 2


# ---------------------------------------------------------------------------
# write_all / read_all round-trip
# ---------------------------------------------------------------------------


class TestWriteAllReadAll:
    def test_write_all_and_read_all_roundtrip(self, tmp_path):
        sm = _sm(tmp_path)
        data = {"x": 1, "y": "hello", "z": [True, None, 3.14]}
        sm.write_all(data)
        assert sm.read_all() == data

    def test_write_all_overwrites_previous_content(self, tmp_path):
        sm = _sm(tmp_path)
        sm.write_all({"old": "data"})
        sm.write_all({"new": "data"})
        assert sm.read_all() == {"new": "data"}

    def test_write_all_empty_dict_clears_file(self, tmp_path):
        sm = _sm(tmp_path)
        sm.set("something", 99)
        sm.write_all({})
        assert sm.read_all() == {}


# ---------------------------------------------------------------------------
# Concurrent writes
# ---------------------------------------------------------------------------


class TestConcurrentWrites:
    def test_concurrent_writes_produce_correct_state(self, tmp_path):
        """10 threads each set a unique key; all 10 must survive with correct values."""
        sm = _sm(tmp_path)
        threads = [
            threading.Thread(target=sm.set, args=(f"key{i}", i)) for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        result = sm.read_all()
        assert len(result) == 10
        for i in range(10):
            assert result[f"key{i}"] == i

    def test_concurrent_mixed_ops_do_not_corrupt_file(self, tmp_path):
        """Interleaved sets and gets must not raise or corrupt the JSON file."""
        sm = _sm(tmp_path)
        sm.set("shared", 0)
        errors: list[Exception] = []

        def writer(i: int) -> None:
            try:
                sm.set(f"w{i}", i)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def reader() -> None:
            try:
                sm.read_all()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Unexpected exceptions during concurrent ops: {errors}"
