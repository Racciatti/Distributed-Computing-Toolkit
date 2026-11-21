"""Tests for the Maekawa quorum-based distributed mutex (MaekawaMutexNode).

Port / node-ID allocation (port = 5000 + node_id):
    test_single_node_self_quorum                : ID 80           → port 5080
    test_two_node_sequential_acquire_release    : IDs 81, 82      → ports 5081-5082
    test_mutual_exclusion_no_overlap            : IDs 83, 84, 85  → ports 5083-5085
    test_deferred_grant_on_release              : IDs 86, 87, 88  → ports 5086-5088
    test_handle_request_defers_when_voted       : ID 89           → no socket (unit test)
    test_release_processes_queue                : ID 89           → no socket (unit test)

Quorum assignments (intersection property: any two quorums share at least one member):
    3-node cluster A (IDs 83, 84, 85):
        node 83 → quorum [83, 84]
        node 84 → quorum [83, 84, 85]
        node 85 → quorum [84, 85]
    3-node cluster B (IDs 86, 87, 88):
        node 86 → quorum [86, 87]
        node 87 → quorum [86, 87, 88]
        node 88 → quorum [87, 88]
"""

import threading
import time

from src.messaging import Message
from src.mutex import MaekawaMutexNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CapturingMaekawaMutexNode(MaekawaMutexNode):
    """MaekawaMutexNode that records send() calls without performing network I/O.

    Used for unit tests that exercise internal handler logic without requiring
    live TCP sockets.
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


class TestMaekawaMutexNodeUnit:
    """Tests that exercise internal state transitions without live sockets."""

    # ------------------------------------------------------------------
    # 5. _handle_request defers when voted
    # ------------------------------------------------------------------

    def test_handle_request_defers_when_voted(self):
        """_handle_request queues the incoming request when _voted_for is already set.

        Pre-condition: node 89 has already granted its vote (_voted_for is not None).
        An MK_REQUEST arrives from node 86 (clock=5).
        Expected: the request is appended to _pending_queue; no MK_GRANT is sent.
        """
        node = CapturingMaekawaMutexNode(89, [89], quorum=[89])

        # Simulate having already cast a vote
        with node._maekawa_lock:
            node._voted_for = 89  # voted for self

        incoming = Message(
            type="MK_REQUEST",
            sender_id=86,
            payload={"request_clock": 5},
            lamport_clock=5,
        )
        node._handle_request(incoming)

        # Request must be queued
        assert len(node._pending_queue) == 1, (
            f"Expected 1 queued request, got: {node._pending_queue}"
        )
        assert node._pending_queue[0] == (5, 86), (
            f"Queued entry mismatch: {node._pending_queue[0]}"
        )

        # No MK_GRANT should have been sent
        grants_sent = [m for _, m, _ in node.sent_messages if m == "MK_GRANT"]
        assert len(grants_sent) == 0, (
            f"MK_GRANT was sent despite _voted_for already being set: {node.sent_messages}"
        )

    def test_handle_request_grants_when_not_voted(self):
        """_handle_request grants immediately when _voted_for is None.

        Verifies the positive path of the same code branch tested above.
        """
        node = CapturingMaekawaMutexNode(89, [89], quorum=[89])
        # Default state: _voted_for is None

        incoming = Message(
            type="MK_REQUEST",
            sender_id=86,
            payload={"request_clock": 5},
            lamport_clock=5,
        )
        node._handle_request(incoming)

        assert node._pending_queue == [], (
            f"Queue should be empty when request was granted: {node._pending_queue}"
        )
        grants_sent = [(t, m) for t, m, _ in node.sent_messages if m == "MK_GRANT"]
        assert (86, "MK_GRANT") in grants_sent, (
            f"Expected MK_GRANT to node 86, got: {node.sent_messages}"
        )
        assert node._voted_for == 86, f"_voted_for should be 86, got: {node._voted_for}"

    # ------------------------------------------------------------------
    # 6. Release processes queue
    # ------------------------------------------------------------------

    def test_release_processes_queue(self):
        """_handle_release drains _pending_queue and grants the highest-priority waiter.

        Pre-condition: _voted_for=89 (in CS), _pending_queue=[(5, 86)].
        An MK_RELEASE arrives from node 89.
        Expected: _pending_queue is empty, _voted_for=86, MK_GRANT sent to node 86.
        """
        node = CapturingMaekawaMutexNode(89, [89], quorum=[89])

        with node._maekawa_lock:
            node._voted_for = 89  # currently voted for self (node in CS)
            node._pending_queue = [(5, 86)]  # node 86 is waiting

        release_msg = Message(
            type="MK_RELEASE",
            sender_id=89,
            payload={},
            lamport_clock=10,
        )
        node._handle_release(release_msg)

        # Queue must be empty after processing
        assert node._pending_queue == [], (
            f"_pending_queue should be empty after release, got: {node._pending_queue}"
        )

        # _voted_for must be set to the node that was just granted
        assert node._voted_for == 86, (
            f"_voted_for should be 86 after dequeue, got: {node._voted_for}"
        )

        # MK_GRANT must have been sent to node 86
        grants_sent = [(t, m) for t, m, _ in node.sent_messages if m == "MK_GRANT"]
        assert (86, "MK_GRANT") in grants_sent, (
            f"Expected MK_GRANT to node 86, got: {node.sent_messages}"
        )

    def test_release_with_empty_queue_clears_vote(self):
        """_handle_release with an empty queue only clears _voted_for; no grant is sent."""
        node = CapturingMaekawaMutexNode(89, [89], quorum=[89])

        with node._maekawa_lock:
            node._voted_for = 89
            node._pending_queue = []

        release_msg = Message(
            type="MK_RELEASE",
            sender_id=89,
            payload={},
            lamport_clock=5,
        )
        node._handle_release(release_msg)

        assert node._voted_for is None, (
            f"_voted_for should be None after release with empty queue, got: {node._voted_for}"
        )
        assert node._pending_queue == []
        grants_sent = [m for _, m, _ in node.sent_messages if m == "MK_GRANT"]
        assert len(grants_sent) == 0, (
            f"No MK_GRANT expected with empty queue, got: {node.sent_messages}"
        )

    def test_release_picks_highest_priority_from_queue(self):
        """_handle_release grants the request with the lowest (clock, node_id) when multiple are queued."""
        node = CapturingMaekawaMutexNode(89, [89], quorum=[89])

        with node._maekawa_lock:
            node._voted_for = 89
            # Two pending requests: node 87 with clock 10, node 86 with clock 5
            node._pending_queue = [(10, 87), (5, 86)]

        release_msg = Message(
            type="MK_RELEASE",
            sender_id=89,
            payload={},
            lamport_clock=15,
        )
        node._handle_release(release_msg)

        # node 86 (clock=5) has higher priority than node 87 (clock=10)
        assert node._voted_for == 86, (
            f"Expected _voted_for=86 (lower clock wins), got: {node._voted_for}"
        )
        assert len(node._pending_queue) == 1, (
            f"One request should remain in queue, got: {node._pending_queue}"
        )
        grants_sent = [(t, m) for t, m, _ in node.sent_messages if m == "MK_GRANT"]
        assert (86, "MK_GRANT") in grants_sent, (
            f"MK_GRANT should go to node 86, got: {node.sent_messages}"
        )


# ---------------------------------------------------------------------------
# Integration tests — nodes start listeners and communicate over TCP
# ---------------------------------------------------------------------------


class TestMaekawaMutexNodeIntegration:
    """Full integration tests using live TCP sockets."""

    def setup_method(self):
        """Initialise per-test node registry."""
        self._nodes = []

    def teardown_method(self, method):
        """Stop all nodes created by this test to release their server sockets."""
        for n in self._nodes:
            n.stop()
        self._nodes = []
        import time

        time.sleep(0.15)  # allow OS to release ports

    # ------------------------------------------------------------------
    # 1. Single node with self-only quorum
    # ------------------------------------------------------------------

    def test_single_node_self_quorum(self):
        """A node whose quorum contains only itself acquires immediately.

        Node 80, quorum=[80]. The self-grant satisfies the full quorum
        (size 1), so acquire() must return without blocking on the network.
        After acquire(), _in_cs must be True.
        """
        node = MaekawaMutexNode(80, [80], quorum=[80])
        self._nodes.append(node)
        node.start()
        time.sleep(0.3)

        acquired = threading.Event()

        def do_acquire():
            node.acquire()
            acquired.set()

        t = threading.Thread(target=do_acquire, daemon=True)
        t.start()
        t.join(timeout=8.0)

        assert acquired.is_set(), (
            "acquire() did not return for a self-only quorum within 8 s"
        )
        assert node._in_cs is True, (
            f"Expected _in_cs=True after acquire(), got: {node._in_cs}"
        )

    # ------------------------------------------------------------------
    # 2. Two-node acquire and release
    # ------------------------------------------------------------------

    def test_two_node_sequential_acquire_release(self):
        """Node 81 acquires and releases; then node 82 acquires successfully.

        Quorums: both nodes use [81, 82] so each node needs a grant from
        the other plus its own self-grant to enter the CS.

        Each acquire() is driven from a background thread with a 15-second
        join timeout so the test fails fast (instead of hanging) if the
        acquire blocks indefinitely.
        """
        all_ids = [81, 82]
        node_81 = MaekawaMutexNode(81, all_ids, quorum=[81, 82])
        node_82 = MaekawaMutexNode(82, all_ids, quorum=[81, 82])
        self._nodes.extend([node_81, node_82])
        _start_nodes([node_81, node_82])

        # --- node 81: acquire ---
        acquire_81_done = threading.Event()
        acquire_81_error: list[BaseException] = []

        def _acquire_81():
            try:
                node_81.acquire()
                acquire_81_done.set()
            except BaseException as exc:  # noqa: BLE001
                acquire_81_error.append(exc)

        t_acquire_81 = threading.Thread(target=_acquire_81, daemon=True)
        t_acquire_81.start()
        t_acquire_81.join(timeout=15.0)

        assert not t_acquire_81.is_alive(), (
            "node_81.acquire() did not complete within 15 s — possible deadlock"
        )
        if acquire_81_error:
            raise acquire_81_error[0]
        assert acquire_81_done.is_set(), "node_81.acquire() never signalled done"
        assert node_81._in_cs is True, "Node 81 should be in CS after acquire()"

        node_81.release()
        assert node_81._in_cs is False, "Node 81 should not be in CS after release()"

        # --- node 82: acquire ---
        acquire_82_done = threading.Event()
        acquire_82_error: list[BaseException] = []

        def _acquire_82():
            try:
                node_82.acquire()
                acquire_82_done.set()
            except BaseException as exc:  # noqa: BLE001
                acquire_82_error.append(exc)

        t_acquire_82 = threading.Thread(target=_acquire_82, daemon=True)
        t_acquire_82.start()
        t_acquire_82.join(timeout=15.0)

        assert not t_acquire_82.is_alive(), (
            "node_82.acquire() did not complete within 15 s — possible deadlock"
        )
        if acquire_82_error:
            raise acquire_82_error[0]
        assert acquire_82_done.is_set(), "node_82.acquire() never signalled done"
        assert node_82._in_cs is True, "Node 82 should be in CS after acquire()"

        node_82.release()
        assert node_82._in_cs is False, "Node 82 should not be in CS after release()"

    # ------------------------------------------------------------------
    # 3. Mutual exclusion — no two nodes in CS simultaneously
    # ------------------------------------------------------------------

    def test_mutual_exclusion_no_overlap(self):
        """Three nodes calling acquire() concurrently must never overlap in the CS.

        Nodes 83, 84, 85 with quorums:
            83 → [83, 84]
            84 → [83, 84, 85]
            85 → [84, 85]
        Any two quorums share at least one member. The main thread polls every
        10 ms to detect simultaneous _in_cs=True on any two nodes.
        """
        all_ids = [83, 84, 85]
        node_83 = MaekawaMutexNode(83, all_ids, quorum=[83, 84])
        node_84 = MaekawaMutexNode(84, all_ids, quorum=[83, 84, 85])
        node_85 = MaekawaMutexNode(85, all_ids, quorum=[84, 85])
        nodes = [node_83, node_84, node_85]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        overlap_detected = threading.Event()

        def acquire_release(node):
            node.acquire()
            time.sleep(0.05)  # hold CS briefly to expose races
            node.release()

        threads = [
            threading.Thread(target=acquire_release, args=(n,), daemon=True)
            for n in nodes
        ]

        for t in threads:
            t.start()

        # Poll for simultaneous CS occupancy
        deadline = time.time() + 30.0
        while time.time() < deadline:
            in_cs_count = sum(1 for n in nodes if n._in_cs)
            if in_cs_count > 1:
                overlap_detected.set()
                break
            if all(not t.is_alive() for t in threads):
                break
            time.sleep(0.01)

        for t in threads:
            t.join(timeout=30.0)

        assert not overlap_detected.is_set(), (
            "Mutual exclusion violated: two nodes were in CS simultaneously"
        )
        assert all(not t.is_alive() for t in threads), (
            "Not all acquire/release threads completed within the allotted time"
        )

    # ------------------------------------------------------------------
    # 4. Deferred grant on release
    # ------------------------------------------------------------------

    def test_deferred_grant_on_release(self):
        """Node 87 holds CS while node 86 blocks; node 86 enters after node 87 releases.

        Nodes 86, 87, 88 with quorums:
            86 → [86, 87]
            87 → [86, 87, 88]
            88 → [87, 88]

        Sequence:
        1. Node 87 acquires and enters CS (needs grants from 86, 87-self, 88).
        2. Node 86 calls acquire() in a background thread — it self-grants but
           must wait for node 87's grant. Node 87 has already voted (for itself),
           so it queues node 86's request and node 86 blocks.
        3. Node 87 releases — it processes its pending queue and grants node 86.
        4. Node 86's acquire() unblocks and _in_cs becomes True.
        """
        all_ids = [86, 87, 88]
        node_86 = MaekawaMutexNode(86, all_ids, quorum=[86, 87])
        node_87 = MaekawaMutexNode(87, all_ids, quorum=[86, 87, 88])
        node_88 = MaekawaMutexNode(88, all_ids, quorum=[87, 88])
        self._nodes.extend([node_86, node_87, node_88])
        _start_nodes([node_86, node_87, node_88])

        node_87_in_cs = threading.Event()

        def node_87_acquire():
            node_87.acquire()
            node_87_in_cs.set()

        t87 = threading.Thread(target=node_87_acquire, daemon=True)
        t87.start()

        assert node_87_in_cs.wait(timeout=8.0), "Node 87 did not enter CS within 8 s"
        assert node_87._in_cs is True

        # Node 86 now tries to acquire — it self-grants but is blocked waiting
        # for node 87's grant (node 87 has already voted, so it will queue 86).
        node_86_acquired = threading.Event()

        def node_86_acquire():
            node_86.acquire()
            node_86_acquired.set()

        t86 = threading.Thread(target=node_86_acquire, daemon=True)
        t86.start()

        # Allow node 86's MK_REQUEST to reach node 87 and be queued there
        time.sleep(0.5)

        assert node_86._in_cs is False, (
            "Node 86 entered CS while node 87 still holds it — mutual exclusion violated"
        )

        # Release node 87 — it must dequeue and grant node 86
        node_87.release()
        assert node_87._in_cs is False

        assert node_86_acquired.wait(timeout=8.0), (
            "Node 86 did not enter CS after node 87 released within 8 s"
        )
        assert node_86._in_cs is True, (
            "Node 86 should be in CS after receiving deferred grant from node 87"
        )

        node_86.release()
        t87.join(timeout=4.0)
        t86.join(timeout=4.0)
