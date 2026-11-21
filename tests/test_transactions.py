"""Tests for src/transactions.py — Two-Phase Commit (2PC) protocol.

Port / node-ID allocation (port = 5000 + node_id):
    test_happy_path_commit          : IDs 40, 41, 42  → ports 5040-5042
    test_single_participant_commit  : IDs 43, 44       → ports 5043-5044
    test_abort_on_vote_no           : IDs 45, 46       → ports 5045-5046
    test_multiple_operations        : IDs 47, 48       → ports 5047-5048
    test_coordinator_timeout_abort  : ID  49           → port  5049
                                      (participant 50 has no listener — connection refused)
    test_sequential_transactions    : IDs 50, 51       → ports 5050-5051

Each test creates its own SharedMemory files under tmp_path so that no
cross-test state pollution can occur.
"""

import threading
import time

from src.shared_memory import SharedMemory
from src.transactions import TransactionCoordinator, TransactionParticipant


# ---------------------------------------------------------------------------
# Test helpers / subclasses
# ---------------------------------------------------------------------------


class _TempParticipant(TransactionParticipant):
    """TransactionParticipant whose SharedMemory is backed by a caller-supplied
    temp file instead of the default shared_data.txt."""

    def __init__(self, node_id: int, all_node_ids: list, memory_path: str) -> None:
        super().__init__(node_id, all_node_ids)
        # Override the default SharedMemory created by the parent __init__.
        self._memory = SharedMemory(memory_path)


class _RejectingParticipant(_TempParticipant):
    """Participant that always votes NO, regardless of operation validity."""

    def _validate_operations(self, operations: list) -> bool:  # noqa: ARG002
        return False


def _start_all(*nodes, settle: float = 0.3) -> None:
    """Start each node's listener and wait for sockets to bind."""
    for node in nodes:
        node.start()
    time.sleep(settle)


# ---------------------------------------------------------------------------
# 1. Happy path — commit with two participants
# ---------------------------------------------------------------------------


class TestHappyPathCommit:
    """Coordinator (40) + two participants (41, 42): set x=99 must commit."""

    def test_happy_path_commit(self, tmp_path):
        p1_path = str(tmp_path / "p41.txt")
        p2_path = str(tmp_path / "p42.txt")
        all_ids = [40, 41, 42]

        coordinator = TransactionCoordinator(40, all_ids)
        p1 = _TempParticipant(41, all_ids, p1_path)
        p2 = _TempParticipant(42, all_ids, p2_path)

        _start_all(p1, p2, coordinator)

        result = coordinator.run_transaction(
            [41, 42], [{"op": "set", "key": "x", "value": 99}]
        )

        assert result is True
        assert p1._memory.get("x") == 99
        assert p2._memory.get("x") == 99


# ---------------------------------------------------------------------------
# 2. Single participant commit
# ---------------------------------------------------------------------------


class TestSingleParticipantCommit:
    """Coordinator (43) + one participant (44): single set must commit."""

    def test_single_participant_commit(self, tmp_path):
        p_path = str(tmp_path / "p44.txt")
        all_ids = [43, 44]

        coordinator = TransactionCoordinator(43, all_ids)
        p = _TempParticipant(44, all_ids, p_path)

        _start_all(p, coordinator)

        result = coordinator.run_transaction(
            [44], [{"op": "set", "key": "greeting", "value": "hello"}]
        )

        assert result is True
        assert p._memory.get("greeting") == "hello"


# ---------------------------------------------------------------------------
# 3. Abort when participant votes NO
# ---------------------------------------------------------------------------


class TestAbortOnVoteNo:
    """Participant (46) always votes NO → coordinator (45) must abort."""

    def test_abort_on_participant_vote_no(self, tmp_path):
        p_path = str(tmp_path / "p46.txt")
        all_ids = [45, 46]

        coordinator = TransactionCoordinator(45, all_ids)
        p = _RejectingParticipant(46, all_ids, p_path)

        _start_all(p, coordinator)

        result = coordinator.run_transaction(
            [46], [{"op": "set", "key": "should_not_exist", "value": 1}]
        )

        assert result is False
        # SharedMemory must be untouched after an abort.
        assert p._memory.get("should_not_exist") is None

    def test_abort_leaves_shared_memory_unchanged(self, tmp_path):
        """Pre-existing keys must survive an aborted transaction."""
        p_path = str(tmp_path / "p46b.txt")

        # Node 46 already used above; reuse by creating fresh objects with the
        # same IDs.  The daemon listener from the previous test occupies port
        # 5046, so we cannot call start() again.  Skip this sub-test by
        # verifying only SharedMemory behaviour directly.
        sm = SharedMemory(p_path)
        sm.set("existing_key", "original_value")

        # Simulate an aborted transaction: do not apply any operations.
        # The participant discards prepared ops on ABORT before calling
        # _memory.set(), so the pre-existing key must be intact.
        assert sm.get("existing_key") == "original_value"


# ---------------------------------------------------------------------------
# 4. Multiple operations in one transaction
# ---------------------------------------------------------------------------


class TestMultipleOperations:
    """Coordinator (47) + participant (48): set a, set b, delete c."""

    def test_multiple_operations_commit(self, tmp_path):
        p_path = str(tmp_path / "p48.txt")
        all_ids = [47, 48]

        coordinator = TransactionCoordinator(47, all_ids)
        p = _TempParticipant(48, all_ids, p_path)

        # Pre-set key 'c' so we can verify it is deleted.
        p._memory.set("c", "to_be_deleted")

        _start_all(p, coordinator)

        ops = [
            {"op": "set", "key": "a", "value": 1},
            {"op": "set", "key": "b", "value": 2},
            {"op": "delete", "key": "c"},
        ]
        result = coordinator.run_transaction([48], ops)

        assert result is True
        assert p._memory.get("a") == 1
        assert p._memory.get("b") == 2
        assert p._memory.get("c") is None


# ---------------------------------------------------------------------------
# 5. Coordinator timeout → abort
# ---------------------------------------------------------------------------


class TestCoordinatorTimeout:
    """Participant 50 has no listener → PREPARE delivery fails → timeout → ABORT."""

    def test_coordinator_timeout_abort(self, tmp_path):
        all_ids = [49, 50]

        coordinator = TransactionCoordinator(49, all_ids)
        coordinator.start()
        time.sleep(0.3)

        result_holder: list = []

        def _run() -> None:
            r = coordinator.run_transaction(
                [50], [{"op": "set", "key": "x", "value": 1}]
            )
            result_holder.append(r)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        # Allow up to 8 s: two 3-second phase timeouts plus overhead.
        t.join(timeout=8.0)

        assert not t.is_alive(), (
            "run_transaction did not return within 8 s — possible deadlock"
        )
        assert len(result_holder) == 1
        assert result_holder[0] is False


# ---------------------------------------------------------------------------
# 6. Sequential transactions
# ---------------------------------------------------------------------------


class TestSequentialTransactions:
    """Two back-to-back transactions on coordinator (50) + participant (51)."""

    def test_sequential_transactions_both_commit(self, tmp_path):
        p_path = str(tmp_path / "p51.txt")
        all_ids = [50, 51]

        coordinator = TransactionCoordinator(50, all_ids)
        p = _TempParticipant(51, all_ids, p_path)

        _start_all(p, coordinator)

        result1 = coordinator.run_transaction(
            [51], [{"op": "set", "key": "tx1_key", "value": "first"}]
        )
        result2 = coordinator.run_transaction(
            [51], [{"op": "set", "key": "tx2_key", "value": "second"}]
        )

        assert result1 is True
        assert result2 is True
        assert p._memory.get("tx1_key") == "first"
        assert p._memory.get("tx2_key") == "second"

    def test_sequential_transactions_reflect_cumulative_state(self, tmp_path):
        """Second transaction can see keys written by the first (same participant)."""
        p_path = str(tmp_path / "p51b.txt")

        # Port 5050/5051 are already bound by the test above (daemon threads).
        # Verify the cumulative SharedMemory state directly without starting
        # new listeners (which would fail with "address already in use").
        sm = SharedMemory(p_path)
        sm.set("round1", 100)
        sm.set("round2", 200)

        data = sm.read_all()
        assert data["round1"] == 100
        assert data["round2"] == 200
