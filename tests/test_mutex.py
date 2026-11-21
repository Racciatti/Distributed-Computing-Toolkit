"""Tests for the Ricart-Agrawala distributed mutex (MutexNode).

Port / node-ID allocation (port = 5000 + node_id):
    test_single_node_acquires_immediately   : ID 30         → port 5030
    test_sequential_acquires                : IDs 31, 32    → ports 5031-5032
    test_mutual_exclusion_no_overlap        : IDs 33, 34, 35 → ports 5033-5035
    test_deferred_reply_sent_on_release     : IDs 36, 37    → ports 5036-5037
    test_priority_rule_lower_clock_wins     : ID 38         → no socket (unit test)
    test_priority_rule_higher_clock_defers  : ID 38         → no socket (unit test)
    test_release_sends_deferred_replies     : ID 39         → no socket (unit test)
"""

import threading
import time

from src.messaging import Message
from src.mutex import MutexNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CapturingMutexNode(MutexNode):
    """MutexNode that records send() calls without performing network I/O.

    Used for unit tests that exercise _handle_request and release() logic
    without requiring live TCP sockets.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent_messages: list[tuple[int, str, dict]] = []
        self._send_lock = threading.Lock()

    def send(self, target_id: int, msg_type: str, payload: dict) -> None:
        """Record the outgoing message instead of sending it over the network."""
        with self._send_lock:
            self.sent_messages.append((target_id, msg_type, payload))


def _start_nodes(nodes: list) -> None:
    """Call start() on every node and allow sockets time to bind."""
    for node in nodes:
        node.start()
    time.sleep(0.3)


# ---------------------------------------------------------------------------
# Unit tests — no network I/O; start() is never called
# ---------------------------------------------------------------------------


class TestMutexNodeUnit:
    """Tests that exercise internal state transitions without live sockets."""

    # ------------------------------------------------------------------
    # 5. Priority rule — lower clock wins
    # ------------------------------------------------------------------

    def test_priority_rule_lower_clock_wins(self):
        """A requester with a lower Lamport clock has higher priority and gets an immediate reply.

        Node 38 is in the 'wanting' state (_requesting=True, _in_cs=False,
        _request_clock=10). An incoming MUTEX_REQUEST arrives from node 37
        with clock=5 (lower clock → higher priority). Node 38 must reply
        immediately and must NOT add node 37 to _deferred.
        """
        node = CapturingMutexNode(38, [37, 38])

        with node._mutex_lock:
            node._requesting = True
            node._in_cs = False
            node._request_clock = 10

        incoming = Message(
            type="MUTEX_REQUEST",
            sender_id=37,
            payload={"request_clock": 5},
            lamport_clock=5,
        )
        node._handle_request(incoming)

        sent = [(t, m) for t, m, _ in node.sent_messages]
        assert (37, "MUTEX_REPLY") in sent, (
            f"Expected MUTEX_REPLY to node 37, got: {node.sent_messages}"
        )
        assert 37 not in node._deferred, (
            "Node 37 was incorrectly added to _deferred despite having higher priority"
        )

    def test_priority_rule_higher_clock_defers(self):
        """A requester with a higher Lamport clock has lower priority and the reply is deferred.

        Node 38 is in the 'wanting' state with _request_clock=5. An incoming
        MUTEX_REQUEST arrives from node 37 with clock=10 (higher clock → lower
        priority). Node 38 must defer the reply.
        """
        node = CapturingMutexNode(38, [37, 38])

        with node._mutex_lock:
            node._requesting = True
            node._in_cs = False
            node._request_clock = 5

        incoming = Message(
            type="MUTEX_REQUEST",
            sender_id=37,
            payload={"request_clock": 10},
            lamport_clock=10,
        )
        node._handle_request(incoming)

        assert 37 in node._deferred, (
            "Node 37 was not added to _deferred despite having lower priority"
        )
        reply_sent = any(mtype == "MUTEX_REPLY" for _, mtype, _ in node.sent_messages)
        assert not reply_sent, (
            f"MUTEX_REPLY was sent despite node 37 having lower priority: {node.sent_messages}"
        )

    def test_priority_rule_in_cs_always_defers(self):
        """A node currently in the CS always defers regardless of the requester's clock.

        Node 38 holds the CS (_in_cs=True, _requesting=True). An incoming
        request with a very low clock (priority 1) must still be deferred.
        """
        node = CapturingMutexNode(38, [37, 38])

        with node._mutex_lock:
            node._requesting = True
            node._in_cs = True
            node._request_clock = 10

        incoming = Message(
            type="MUTEX_REQUEST",
            sender_id=37,
            payload={"request_clock": 1},
            lamport_clock=1,
        )
        node._handle_request(incoming)

        assert 37 in node._deferred, (
            "Node in CS must always defer, but node 37 was not added to _deferred"
        )
        reply_sent = any(mtype == "MUTEX_REPLY" for _, mtype, _ in node.sent_messages)
        assert not reply_sent, "No MUTEX_REPLY should be sent while node is in CS"

    # ------------------------------------------------------------------
    # 6. Release sends deferred replies
    # ------------------------------------------------------------------

    def test_release_sends_deferred_replies(self):
        """release() drains _deferred, leaves node outside CS, and sends replies.

        Pre-condition : _deferred=[1, 2], _in_cs=True, _requesting=True.
        Post-condition: _deferred=[], _in_cs=False, _requesting=False.
        MUTEX_REPLY must have been attempted for both node 1 and node 2.
        """
        node = CapturingMutexNode(39, [39])

        with node._mutex_lock:
            node._in_cs = True
            node._requesting = True
            node._deferred = [1, 2]

        node.release()

        assert node._in_cs is False, "Node should not be in CS after release()"
        assert node._requesting is False, "_requesting should be False after release()"
        assert node._deferred == [], f"_deferred should be empty, got: {node._deferred}"

        replied_to = [
            target for target, mtype, _ in node.sent_messages if mtype == "MUTEX_REPLY"
        ]
        assert sorted(replied_to) == [1, 2], (
            f"Expected MUTEX_REPLY to nodes 1 and 2, got: {node.sent_messages}"
        )

    def test_release_with_no_deferred(self):
        """release() with an empty _deferred list must still clear _in_cs and _requesting."""
        node = CapturingMutexNode(39, [39])

        with node._mutex_lock:
            node._in_cs = True
            node._requesting = True
            node._deferred = []

        node.release()

        assert node._in_cs is False
        assert node._requesting is False
        assert node._deferred == []
        assert node.sent_messages == [], (
            "No messages should be sent when _deferred is empty"
        )

    def test_not_requesting_replies_immediately(self):
        """A node not in the wanting or CS state replies to any request immediately."""
        node = CapturingMutexNode(38, [37, 38])
        # Default state: _requesting=False, _in_cs=False

        incoming = Message(
            type="MUTEX_REQUEST",
            sender_id=37,
            payload={"request_clock": 99},
            lamport_clock=99,
        )
        node._handle_request(incoming)

        sent = [(t, m) for t, m, _ in node.sent_messages]
        assert (37, "MUTEX_REPLY") in sent, (
            f"Expected immediate MUTEX_REPLY when not requesting, got: {node.sent_messages}"
        )
        assert 37 not in node._deferred


# ---------------------------------------------------------------------------
# Integration tests — nodes start listeners and communicate over TCP
# ---------------------------------------------------------------------------


class TestMutexNodeIntegration:
    """Full integration tests using live TCP sockets."""

    # ------------------------------------------------------------------
    # 1. Single node acquires immediately
    # ------------------------------------------------------------------

    def test_single_node_acquires_immediately(self):
        """A lone node with no peers should acquire the CS without blocking.

        Node 30 has all_node_ids=[30]. Broadcast reaches no one and N-1 = 0
        replies are needed, so acquire() must return immediately.

        NOTE: The current implementation calls _cs_event.wait() unconditionally
        even when there are no peers, which causes acquire() to block forever.
        If this test times out, it indicates a missing zero-peer short-circuit
        in MutexNode.acquire().
        """
        node = MutexNode(30, [30])
        node.start()
        time.sleep(0.3)

        acquired = threading.Event()

        def do_acquire():
            node.acquire()
            acquired.set()

        t = threading.Thread(target=do_acquire, daemon=True)
        t.start()
        t.join(timeout=5.0)

        assert acquired.is_set(), (
            "acquire() did not return for a single-node cluster within 5 s — "
            "likely missing zero-peer short-circuit in acquire()"
        )
        assert node._in_cs is True

    # ------------------------------------------------------------------
    # 2. Sequential acquires
    # ------------------------------------------------------------------

    def test_sequential_acquires(self):
        """Node 31 acquires and releases; then node 32 acquires without deadlock.

        Verifies the basic acquire/release lifecycle for a two-node cluster.
        Each acquire() is driven from a background thread with a 15-second
        join timeout so the test fails fast instead of hanging if acquire blocks.
        """
        all_ids = [31, 32]
        node_31 = MutexNode(31, all_ids)
        node_32 = MutexNode(32, all_ids)
        _start_nodes([node_31, node_32])

        # --- node 31: acquire ---
        acquire_31_done = threading.Event()
        acquire_31_error: list[BaseException] = []

        def _acquire_31():
            try:
                node_31.acquire()
                acquire_31_done.set()
            except BaseException as exc:  # noqa: BLE001
                acquire_31_error.append(exc)

        t_31 = threading.Thread(target=_acquire_31, daemon=True)
        t_31.start()
        t_31.join(timeout=15.0)

        assert not t_31.is_alive(), (
            "node_31.acquire() did not complete within 15 s — possible deadlock"
        )
        if acquire_31_error:
            raise acquire_31_error[0]
        assert acquire_31_done.is_set(), "node_31.acquire() never signalled done"
        assert node_31._in_cs is True

        node_31.release()
        assert node_31._in_cs is False

        # --- node 32: acquire ---
        acquire_32_done = threading.Event()
        acquire_32_error: list[BaseException] = []

        def _acquire_32():
            try:
                node_32.acquire()
                acquire_32_done.set()
            except BaseException as exc:  # noqa: BLE001
                acquire_32_error.append(exc)

        t_32 = threading.Thread(target=_acquire_32, daemon=True)
        t_32.start()
        t_32.join(timeout=15.0)

        assert not t_32.is_alive(), (
            "node_32.acquire() did not complete within 15 s — possible deadlock"
        )
        if acquire_32_error:
            raise acquire_32_error[0]
        assert acquire_32_done.is_set(), "node_32.acquire() never signalled done"
        assert node_32._in_cs is True

        node_32.release()
        assert node_32._in_cs is False

    # ------------------------------------------------------------------
    # 3. Mutual exclusion — no overlap
    # ------------------------------------------------------------------

    def test_mutual_exclusion_no_overlap(self):
        """Three nodes calling acquire() concurrently must never overlap in the CS.

        Each node holds the CS briefly (50 ms) while the main thread polls
        every 10 ms to detect simultaneous _in_cs=True on any two nodes.
        """
        all_ids = [33, 34, 35]
        nodes = [MutexNode(nid, all_ids) for nid in all_ids]
        _start_nodes(nodes)

        overlap_detected = threading.Event()

        def acquire_release(node):
            node.acquire()
            time.sleep(0.05)  # hold the CS briefly
            node.release()

        threads = [
            threading.Thread(target=acquire_release, args=(node,), daemon=True)
            for node in nodes
        ]

        for t in threads:
            t.start()

        # Poll for simultaneous CS occupancy while threads are alive
        deadline = time.time() + 20.0
        while time.time() < deadline:
            in_cs_count = sum(1 for n in nodes if n._in_cs)
            if in_cs_count > 1:
                overlap_detected.set()
                break
            if all(not t.is_alive() for t in threads):
                break
            time.sleep(0.01)

        for t in threads:
            t.join(timeout=20.0)

        assert not overlap_detected.is_set(), (
            "Mutual exclusion violated: two nodes were in CS simultaneously"
        )
        assert all(not t.is_alive() for t in threads), (
            "Not all acquire/release threads completed within the allotted time"
        )

    # ------------------------------------------------------------------
    # 4. Deferred reply sent on release
    # ------------------------------------------------------------------

    def test_deferred_reply_sent_on_release(self):
        """Node 36 holds CS while node 37 is blocked; node 37 enters after node 36 releases.

        Sequence:
        1. Node 36 acquires and holds CS.
        2. Node 37 calls acquire() in a thread — it must block because node 36 defers.
        3. After confirming node 37 is blocked, node 36 calls release().
        4. Node 37's acquire() must unblock and complete.
        """
        all_ids = [36, 37]
        node_36 = MutexNode(36, all_ids)
        node_37 = MutexNode(37, all_ids)
        _start_nodes([node_36, node_37])

        node_36_in_cs = threading.Event()

        def node_36_acquire():
            node_36.acquire()
            node_36_in_cs.set()

        t36 = threading.Thread(target=node_36_acquire, daemon=True)
        t36.start()

        assert node_36_in_cs.wait(timeout=8.0), "Node 36 did not enter CS within 8 s"
        assert node_36._in_cs is True

        # Now have node 37 request CS while node 36 holds it
        node_37_acquired = threading.Event()

        def node_37_acquire():
            node_37.acquire()
            node_37_acquired.set()

        t37 = threading.Thread(target=node_37_acquire, daemon=True)
        t37.start()

        # Give node 37 enough time to broadcast its request and get deferred
        time.sleep(0.5)

        assert node_37._in_cs is False, (
            "Node 37 entered CS while node 36 still holds it — mutual exclusion violated"
        )

        # Release node 36; it must send the deferred reply to node 37
        node_36.release()
        assert node_36._in_cs is False

        assert node_37_acquired.wait(timeout=8.0), (
            "Node 37 did not enter CS after node 36 released within 8 s"
        )
        assert node_37._in_cs is True

        node_37.release()
        t36.join(timeout=2.0)
        t37.join(timeout=2.0)
