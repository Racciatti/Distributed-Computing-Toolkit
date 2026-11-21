"""Tests for src/lock_manager.py.

Port / node-ID allocation (port = 5000 + node_id):
    Integration tests use IDs 110 (manager), 111, 112 (clients) → ports 5110-5112.
    All LockTable tests are pure unit tests with no networking.
"""

import threading
import time

import pytest

from src.lock_manager import (
    LockClientNode,
    LockManagerNode,
    LockTable,
    release_locks,
    request_lock,
)


# ---------------------------------------------------------------------------
# LockTable unit tests (no networking)
# ---------------------------------------------------------------------------


class TestLockTable:
    def test_acquire_shared_success(self):
        """Fresh table: acquire shared for T1 on 'x' returns True."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True

    def test_two_shared_locks_compatible(self):
        """T1 and T2 both acquire shared on 'x': both return True."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.acquire_shared("x", "T2") is True

    def test_exclusive_blocks_shared(self):
        """T1 holds exclusive on 'x'; T2 tries shared: returns False."""
        table = LockTable()
        assert table.acquire_exclusive("x", "T1") is True
        assert table.acquire_shared("x", "T2") is False

    def test_shared_blocks_exclusive(self):
        """T1 holds shared on 'x'; T2 tries exclusive: returns False."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.acquire_exclusive("x", "T2") is False

    def test_exclusive_blocks_exclusive(self):
        """T1 holds exclusive on 'x'; T2 tries exclusive: returns False."""
        table = LockTable()
        assert table.acquire_exclusive("x", "T1") is True
        assert table.acquire_exclusive("x", "T2") is False

    def test_same_tx_reacquire_sole_shared_promotes_to_exclusive(self):
        """T1 is sole shared holder, T1 tries exclusive: returns True (silent promotion).

        The implementation explicitly promotes a sole shared holder to exclusive
        via acquire_exclusive, rather than requiring the upgrade() path.
        """
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        # T1 is the sole shared holder — acquire_exclusive silently promotes it.
        result = table.acquire_exclusive("x", "T1")
        assert result is True
        holders = table.get_holders("x")
        assert holders["exclusive"] == "T1"
        assert "T1" not in holders["shared"]

    def test_same_tx_exclusive_blocked_when_other_shared_exists(self):
        """T1 tries exclusive while T2 also holds shared: returns False."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.acquire_shared("x", "T2") is True
        # Other tx (T2) still holds shared → exclusive must be denied.
        assert table.acquire_exclusive("x", "T1") is False

    def test_release_clears_locks(self):
        """T1 acquires exclusive, releases, T2 can then acquire exclusive."""
        table = LockTable()
        assert table.acquire_exclusive("x", "T1") is True
        table.release("T1")
        assert table.acquire_exclusive("x", "T2") is True

    def test_release_clears_all_resources_for_tx(self):
        """Releasing T1 removes its locks on every resource it held."""
        table = LockTable()
        table.acquire_shared("a", "T1")
        table.acquire_exclusive("b", "T1")
        table.release("T1")
        assert table.is_locked("a") is False
        assert table.is_locked("b") is False

    def test_upgrade_success(self):
        """T1 is sole shared holder; upgrade('x', 'T1') returns True and converts to exclusive."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.upgrade("x", "T1") is True
        holders = table.get_holders("x")
        assert holders["exclusive"] == "T1"
        assert "T1" not in holders["shared"]

    def test_upgrade_fails_multiple_shared(self):
        """T1 and T2 hold shared; T1 tries upgrade: returns False."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.acquire_shared("x", "T2") is True
        assert table.upgrade("x", "T1") is False

    def test_upgrade_fails_when_tx_not_shared_holder(self):
        """upgrade() returns False when tx_id has no shared lock on the resource."""
        table = LockTable()
        assert table.upgrade("x", "T1") is False

    def test_is_locked_unlocked_resource_is_false(self):
        """Unlocked resource reports False."""
        table = LockTable()
        assert table.is_locked("x") is False

    def test_is_locked_after_acquire_is_true(self):
        """After acquiring any lock, is_locked returns True."""
        table = LockTable()
        table.acquire_shared("x", "T1")
        assert table.is_locked("x") is True

    def test_is_locked_after_release_is_false(self):
        """After releasing all locks, is_locked returns False again."""
        table = LockTable()
        table.acquire_exclusive("x", "T1")
        assert table.is_locked("x") is True
        table.release("T1")
        assert table.is_locked("x") is False

    def test_get_holders_returns_copy(self):
        """get_holders returns the current state without exposing internal mutable sets."""
        table = LockTable()
        table.acquire_shared("x", "T1")
        holders = table.get_holders("x")
        assert isinstance(holders["shared"], list)
        assert "T1" in holders["shared"]
        assert holders["exclusive"] is None

    def test_release_noop_for_unknown_tx(self):
        """Releasing an unknown transaction does not raise."""
        table = LockTable()
        table.release("UNKNOWN")  # must not raise

    def test_shared_lock_idempotent_for_same_tx(self):
        """T1 acquiring shared twice on the same resource returns True both times."""
        table = LockTable()
        assert table.acquire_shared("x", "T1") is True
        assert table.acquire_shared("x", "T1") is True
        # Should only appear once in holders list.
        holders = table.get_holders("x")
        assert holders["shared"].count("T1") == 1


# ---------------------------------------------------------------------------
# Integration test fixtures
# ---------------------------------------------------------------------------

_MANAGER_ID = 110
_CLIENT1_ID = 111
_CLIENT2_ID = 112
_ALL_IDS = [_MANAGER_ID, _CLIENT1_ID, _CLIENT2_ID]


@pytest.fixture(scope="module")
def nodes():
    """Start LockManagerNode and two LockClientNodes; yield them; daemon threads clean up."""
    manager = LockManagerNode(_MANAGER_ID, _ALL_IDS)
    client1 = LockClientNode(_CLIENT1_ID, _ALL_IDS)
    client2 = LockClientNode(_CLIENT2_ID, _ALL_IDS)

    manager.start()
    client1.start()
    client2.start()

    time.sleep(0.3)  # allow listener sockets to bind

    yield manager, client1, client2


# ---------------------------------------------------------------------------
# LockManagerNode integration tests
# ---------------------------------------------------------------------------


class TestLockManagerIntegration:
    def test_request_shared_granted(self, nodes):
        """client1 requests shared lock on 'doc1': request_lock returns True."""
        _, client1, _ = nodes
        result = request_lock(client1, _MANAGER_ID, "tx_shared_1", "doc1", "shared")
        assert result is True

    def test_request_exclusive_granted(self, nodes):
        """client1 requests exclusive lock on 'doc2': request_lock returns True."""
        _, client1, _ = nodes
        result = request_lock(client1, _MANAGER_ID, "tx_excl_1", "doc2", "exclusive")
        assert result is True

    def test_exclusive_then_shared_denied(self, nodes):
        """client1 gets exclusive on 'r1'; client2 requests shared on 'r1': returns False."""
        _, client1, client2 = nodes
        granted = request_lock(client1, _MANAGER_ID, "tx_r1_excl", "r1", "exclusive")
        assert granted is True
        result = request_lock(client2, _MANAGER_ID, "tx_r1_shared", "r1", "shared")
        assert result is False

    def test_release_allows_next(self, nodes):
        """client1 gets exclusive on 'r2', releases; client2 then gets exclusive on 'r2'."""
        _, client1, client2 = nodes
        granted = request_lock(client1, _MANAGER_ID, "tx_r2_c1", "r2", "exclusive")
        assert granted is True
        release_locks(client1, _MANAGER_ID, "tx_r2_c1")
        # Give the manager time to process the release before the next request.
        time.sleep(0.1)
        result = request_lock(client2, _MANAGER_ID, "tx_r2_c2", "r2", "exclusive")
        assert result is True

    def test_concurrent_exclusive_only_one_wins(self, nodes):
        """clients 111 and 112 race for exclusive on 'r3'; exactly one wins."""
        _, client1, client2 = nodes
        results: list[bool] = []
        lock = threading.Lock()

        def attempt(client, tx_id):
            r = request_lock(client, _MANAGER_ID, tx_id, "r3", "exclusive")
            with lock:
                results.append(r)

        t1 = threading.Thread(target=attempt, args=(client1, "tx_r3_c1"))
        t2 = threading.Thread(target=attempt, args=(client2, "tx_r3_c2"))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert len(results) == 2, "Both threads must have completed"
        assert results.count(True) == 1, (
            "Exactly one client must win the exclusive lock"
        )
        assert results.count(False) == 1, "Exactly one client must be denied"


# ---------------------------------------------------------------------------
# request_lock error handling
# ---------------------------------------------------------------------------


def test_request_lock_raises_for_non_client_node():
    """request_lock must raise TypeError if node is not a LockClientNode."""
    plain_node = LockManagerNode(200, [200])
    with pytest.raises(TypeError):
        request_lock(plain_node, 110, "tx1", "res", "shared")
