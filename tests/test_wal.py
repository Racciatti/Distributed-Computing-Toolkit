"""Tests for src/wal.py — WriteAheadLog and RecoverableParticipant.

Port / node-ID allocation (port = 5000 + node_id):
    test_prepare_writes_wal          : coordinator=5120, participant=5121
    test_commit_writes_wal           : coordinator=5122, participant=5123
    test_abort_writes_wal            : coordinator=5124, participant=5125
    test_recovery_aborts_incomplete  : participant=5126 (no network, no start)

All WAL files are written to pytest's tmp_path, never to the real logs/ dir.
"""

import json
import os
import threading
import time

from src.shared_memory import SharedMemory
from src.transactions import TransactionCoordinator, TransactionParticipant
from src.wal import RecoverableParticipant, WriteAheadLog


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class _TempRecoverableParticipant(RecoverableParticipant):
    """RecoverableParticipant that stores WAL files in a caller-supplied directory.

    Bypasses RecoverableParticipant.__init__ so we can inject a custom log_dir
    without touching the real logs/ directory on disk.
    """

    def __init__(
        self,
        node_id: int,
        all_node_ids: list,
        shared_memory_path: str,
        log_dir: str,
    ) -> None:
        # Call the grandparent directly to set up node/clock/logger state
        # without invoking RecoverableParticipant.__init__ (which hardcodes
        # the log dir to "logs").
        TransactionParticipant.__init__(self, node_id, all_node_ids)
        self._memory = SharedMemory(shared_memory_path)
        self._wal = WriteAheadLog(node_id, log_dir)
        self.recover_on_startup()


class _RejectingTempParticipant(_TempRecoverableParticipant):
    """RecoverableParticipant that always votes NO, triggering an ABORT path."""

    def _validate_operations(self, operations: list) -> bool:  # noqa: ARG002
        return False


def _start_all(*nodes, settle: float = 0.3) -> None:
    """Start each node's listener and wait for sockets to bind."""
    for node in nodes:
        node.start()
    time.sleep(settle)


# ---------------------------------------------------------------------------
# WriteAheadLog — unit tests
# ---------------------------------------------------------------------------


class TestWriteAheadLogPrepare:
    """log_prepare writes one correctly shaped JSON line."""

    def test_log_prepare_creates_entry(self, tmp_path):
        ops = [{"op": "set", "key": "x", "value": 1}]
        wal = WriteAheadLog(node_id=1, log_dir=str(tmp_path))
        wal.log_prepare("tx1", ops)

        with open(wal.log_path, encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]

        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["phase"] == "prepare"
        assert entry["tx_id"] == "tx1"
        assert entry["operations"] == ops


class TestWriteAheadLogCommit:
    """log_commit appends a second line with phase='commit'."""

    def test_log_commit_appends(self, tmp_path):
        wal = WriteAheadLog(node_id=2, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [])
        wal.log_commit("tx1")

        with open(wal.log_path, encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]

        assert len(lines) == 2
        assert json.loads(lines[1])["phase"] == "commit"
        assert json.loads(lines[1])["tx_id"] == "tx1"


class TestWriteAheadLogAbort:
    """log_abort appends a second line with phase='abort'."""

    def test_log_abort_appends(self, tmp_path):
        wal = WriteAheadLog(node_id=3, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [])
        wal.log_abort("tx1")

        with open(wal.log_path, encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]

        assert len(lines) == 2
        assert json.loads(lines[1])["phase"] == "abort"
        assert json.loads(lines[1])["tx_id"] == "tx1"


class TestWriteAheadLogRecover:
    """recover() returns correct incomplete-transaction lists."""

    def test_recover_returns_incomplete(self, tmp_path):
        ops = [{"op": "set", "key": "k", "value": 42}]
        wal = WriteAheadLog(node_id=4, log_dir=str(tmp_path))
        wal.log_prepare("tx1", ops)

        incomplete = wal.recover()

        assert len(incomplete) == 1
        assert incomplete[0]["tx_id"] == "tx1"
        assert incomplete[0]["operations"] == ops

    def test_recover_excludes_committed(self, tmp_path):
        wal = WriteAheadLog(node_id=5, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [{"op": "set", "key": "a", "value": 1}])
        wal.log_commit("tx1")

        assert wal.recover() == []

    def test_recover_excludes_aborted(self, tmp_path):
        wal = WriteAheadLog(node_id=6, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [{"op": "set", "key": "a", "value": 1}])
        wal.log_abort("tx1")

        assert wal.recover() == []

    def test_recover_multiple_mixed(self, tmp_path):
        wal = WriteAheadLog(node_id=7, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [{"op": "set", "key": "a", "value": 1}])
        wal.log_commit("tx1")
        wal.log_prepare("tx2", [{"op": "set", "key": "b", "value": 2}])

        incomplete = wal.recover()

        assert len(incomplete) == 1
        assert incomplete[0]["tx_id"] == "tx2"
        assert incomplete[0]["operations"] == [{"op": "set", "key": "b", "value": 2}]

    def test_recover_empty_wal(self, tmp_path):
        """recover() returns an empty list when no WAL file exists."""
        wal = WriteAheadLog(node_id=8, log_dir=str(tmp_path))
        # No log_prepare called — file does not exist yet.
        assert wal.recover() == []


class TestWriteAheadLogClear:
    """clear() removes the WAL file from disk."""

    def test_clear_removes_file(self, tmp_path):
        wal = WriteAheadLog(node_id=9, log_dir=str(tmp_path))
        wal.log_prepare("tx1", [])
        assert os.path.exists(wal.log_path)

        wal.clear()

        assert not os.path.exists(wal.log_path)

    def test_clear_is_idempotent(self, tmp_path):
        """Calling clear() when the file does not exist must not raise."""
        wal = WriteAheadLog(node_id=10, log_dir=str(tmp_path))
        wal.clear()  # file never created — must be a no-op
        wal.clear()  # second call — also safe


class TestWriteAheadLogConcurrentWrites:
    """5 threads each log_prepare a distinct tx_id — all 5 lines must land."""

    def test_concurrent_writes(self, tmp_path):
        wal = WriteAheadLog(node_id=11, log_dir=str(tmp_path))
        tx_ids = [f"tx{i}" for i in range(5)]

        threads = [
            threading.Thread(target=wal.log_prepare, args=(tx_id, []))
            for tx_id in tx_ids
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with open(wal.log_path, encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]

        assert len(lines) == 5
        found_ids = {json.loads(ln)["tx_id"] for ln in lines}
        assert found_ids == set(tx_ids)


# ---------------------------------------------------------------------------
# RecoverableParticipant — integration tests
# ---------------------------------------------------------------------------


class TestRecoverableParticipantIntegration:
    """End-to-end 2PC flows verifying WAL entries at each protocol step."""

    def setup_method(self):
        self._nodes: list = []

    def teardown_method(self):
        for node in self._nodes:
            node.stop()

    # ------------------------------------------------------------------
    # 11. PREPARE phase writes WAL prepare entry
    # ------------------------------------------------------------------

    def test_prepare_writes_wal(self, tmp_path):
        """After a successful VOTE_YES, the participant WAL has a prepare entry."""
        sm_path = str(tmp_path / "sm121.txt")
        log_dir = str(tmp_path / "logs121")
        all_ids = [120, 121]
        ops = [{"op": "set", "key": "k", "value": 42}]

        coordinator = TransactionCoordinator(120, all_ids)
        participant = _TempRecoverableParticipant(121, all_ids, sm_path, log_dir)
        self._nodes.extend([coordinator, participant])

        _start_all(coordinator, participant)
        coordinator.run_transaction([121], ops)

        wal = WriteAheadLog(121, log_dir)
        with open(wal.log_path, encoding="utf-8") as fh:
            entries = [json.loads(ln) for ln in fh if ln.strip()]

        prepare_entries = [e for e in entries if e["phase"] == "prepare"]
        assert len(prepare_entries) == 1
        assert prepare_entries[0]["tx_id"] is not None
        assert prepare_entries[0]["operations"] == ops

    # ------------------------------------------------------------------
    # 12. Full commit path writes both prepare and commit WAL entries
    # ------------------------------------------------------------------

    def test_commit_writes_wal(self, tmp_path):
        """A committed 2PC transaction leaves prepare + commit entries in the WAL."""
        sm_path = str(tmp_path / "sm123.txt")
        log_dir = str(tmp_path / "logs123")
        all_ids = [122, 123]
        ops = [{"op": "set", "key": "x", "value": 1}]

        coordinator = TransactionCoordinator(122, all_ids)
        participant = _TempRecoverableParticipant(123, all_ids, sm_path, log_dir)
        self._nodes.extend([coordinator, participant])

        _start_all(coordinator, participant)
        result = coordinator.run_transaction([123], ops)

        assert result is True

        wal = WriteAheadLog(123, log_dir)
        with open(wal.log_path, encoding="utf-8") as fh:
            entries = [json.loads(ln) for ln in fh if ln.strip()]

        phases = [e["phase"] for e in entries]
        assert "prepare" in phases
        assert "commit" in phases

    # ------------------------------------------------------------------
    # 13. VOTE_NO path writes an abort WAL entry
    # ------------------------------------------------------------------

    def test_abort_writes_wal(self, tmp_path):
        """When the participant votes NO, the ABORT handler writes an abort entry."""
        sm_path = str(tmp_path / "sm125.txt")
        log_dir = str(tmp_path / "logs125")
        all_ids = [124, 125]
        ops = [{"op": "set", "key": "x", "value": 1}]

        coordinator = TransactionCoordinator(124, all_ids)
        # Rejecting participant → VOTE_NO → coordinator sends ABORT → abort logged
        participant = _RejectingTempParticipant(125, all_ids, sm_path, log_dir)
        self._nodes.extend([coordinator, participant])

        _start_all(coordinator, participant)
        result = coordinator.run_transaction([125], ops)

        # Coordinator aborts because participant voted NO.
        assert result is False

        wal = WriteAheadLog(125, log_dir)
        with open(wal.log_path, encoding="utf-8") as fh:
            entries = [json.loads(ln) for ln in fh if ln.strip()]

        phases = [e["phase"] for e in entries]
        assert "abort" in phases

    # ------------------------------------------------------------------
    # 14. Startup recovery aborts any WAL-persisted incomplete transaction
    # ------------------------------------------------------------------

    def test_recovery_aborts_incomplete(self, tmp_path):
        """A prepare entry with no commit/abort is conservatively aborted on startup."""
        log_dir = str(tmp_path / "logs126")
        sm_path = str(tmp_path / "sm126.txt")
        os.makedirs(log_dir, exist_ok=True)

        tx_id = "incomplete_tx_001"
        prepare_entry = {
            "tx_id": tx_id,
            "phase": "prepare",
            "operations": [{"op": "set", "key": "z", "value": 99}],
            "timestamp": time.time(),
        }
        wal_path = os.path.join(log_dir, "wal_126.log")
        with open(wal_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(prepare_entry) + "\n")

        # Creating the participant triggers recover_on_startup(); no need to start
        # the network listener for this test.
        all_ids = [126]
        participant = _TempRecoverableParticipant(126, all_ids, sm_path, log_dir)

        with open(wal_path, encoding="utf-8") as fh:
            entries = [json.loads(ln) for ln in fh if ln.strip()]

        phases = [e["phase"] for e in entries]
        assert "prepare" in phases
        assert "abort" in phases

        abort_entries = [
            e for e in entries if e["phase"] == "abort" and e["tx_id"] == tx_id
        ]
        assert len(abort_entries) == 1
        # The participant's in-memory prepared dict must not retain the tx.
        with participant._prepared_lock:
            assert tx_id not in participant._prepared
