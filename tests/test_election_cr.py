"""Tests for the Chang-Roberts ring election algorithm (ChangRobertsNode).

Port / node-ID allocation (port = 5000 + node_id):
    test_highest_id_wins              : IDs 70, 71, 72 → ports 5070-5072
    test_single_node_elects_itself    : ID  73           → port  5073
    test_token_forwarding             : IDs 74, 75, 76   → ports 5074-5076
    test_two_node_ring                : IDs 77, 78        → ports 5077-5078
    test_reelection                   : IDs 79, 80, 81   → ports 5079-5081
    test_is_coordinator_unit          : no network (unit test)
    test_is_coordinator_after_election: IDs 85, 86       → ports 5085-5086

Note on fault tolerance: the standard Chang-Roberts algorithm requires all nodes
to be reachable. If a node is down (no socket) or silently drops messages, the
election token is lost and no new coordinator can be elected.  test_reelection
therefore tests re-election in a fully-alive cluster rather than simulating a
crashed coordinator, which the current implementation cannot recover from.
"""

import threading
import time

from src.election import ChangRobertsNode
from src.messaging import Message


# ============================================================================
# Helper classes and utilities
# ============================================================================


class TrackingCRNode(ChangRobertsNode):
    """ChangRobertsNode that fires a threading.Event once a coordinator is known.

    Attributes:
        coordinator_event: Set as soon as coordinator_id is established, whether
            this node won (own token returned) or a CR_COORDINATOR was received.
        _disabled: When True, silently drop all incoming messages to simulate a
            crashed process.  The flag is checked inside handle_message() so the
            listener's captured bound-method reference still honours it.
        forwarded_cr_elections: List of candidate_ids that this node chose to
            forward clockwise (candidate_id > own_id path in _handle_cr_election).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.coordinator_event = threading.Event()
        self._disabled = False
        self.forwarded_cr_elections: list[int] = []

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def handle_message(self, msg: Message) -> None:
        if self._disabled:
            return
        super().handle_message(msg)

    def _handle_cr_election(self, msg: Message) -> None:
        candidate_id: int = msg.payload["candidate_id"]
        # Record forwarding intent *before* calling super so the list is
        # accurate even if the send inside super raises an OSError.
        if candidate_id > self.node_id:
            self.forwarded_cr_elections.append(candidate_id)
        super()._handle_cr_election(msg)
        # If the super call declared this node coordinator (own token returned),
        # fire the event so the test thread can unblock.
        if self.coordinator_id == self.node_id:
            self.coordinator_event.set()

    def _handle_cr_coordinator(self, msg: Message) -> None:
        super()._handle_cr_coordinator(msg)
        self.coordinator_event.set()


# ------------------------------------------------------------------
# Shared utilities
# ------------------------------------------------------------------


def _start_nodes(nodes: list[TrackingCRNode]) -> None:
    """Call start() on every node and wait 0.3 s for sockets to bind."""
    for node in nodes:
        node.start()
    time.sleep(0.3)


def _trigger_elections(nodes: list[TrackingCRNode]) -> list[threading.Thread]:
    """Spawn a daemon thread that calls start_election() on each node."""
    threads = []
    for node in nodes:
        t = threading.Thread(target=node.start_election, daemon=True)
        t.start()
        threads.append(t)
    return threads


def _all_coordinator_events_set(
    nodes: list[TrackingCRNode], timeout: float = 8.0
) -> bool:
    """Return True if every node's coordinator_event fires within *timeout* s."""
    return all(node.coordinator_event.wait(timeout=timeout) for node in nodes)


# ============================================================================
# Tests
# ============================================================================


class TestChangRoberts:
    """Integration and unit tests for the Chang-Roberts ring election."""

    def setup_method(self):
        """Initialise per-test node registry."""
        self._nodes: list = []

    def teardown_method(self, method):
        """Stop all nodes created by this test to release their server sockets."""
        for n in self._nodes:
            n.stop()
        self._nodes = []
        time.sleep(0.15)  # allow OS to release ports

    # ------------------------------------------------------------------
    # 1. Highest ID wins — 3-node ring
    # ------------------------------------------------------------------

    def test_highest_id_wins(self):
        """All nodes start elections; the node with the highest ID (72) must win.

        Ring (sorted ascending, clockwise): 70 → 71 → 72 → 70.

        Node 72's token travels clockwise past 70 and 71 without being discarded
        (72 > 70, 72 > 71), returns to 72, and triggers the CR_COORDINATOR
        broadcast.  All other tokens (70, 71) are discarded before completing
        the ring.
        """
        all_ids = [70, 71, 72]
        nodes = [TrackingCRNode(nid, all_ids) for nid in all_ids]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        _trigger_elections(nodes)

        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Not all nodes learned the coordinator within the timeout"
        )
        for node in nodes:
            assert node.coordinator_id == 72, (
                f"Node {node.node_id} has wrong coordinator_id: {node.coordinator_id}"
            )

    # ------------------------------------------------------------------
    # 2. Single node elects itself
    # ------------------------------------------------------------------

    def test_single_node_elects_itself(self):
        """A single-node ring must elect itself coordinator.

        Node 73's right_neighbor wraps to itself (ring size 1).  The
        CR_ELECTION token immediately returns and triggers self-declaration.
        """
        node = TrackingCRNode(73, [73])
        self._nodes.append(node)
        node.start()
        time.sleep(0.3)

        t = threading.Thread(target=node.start_election, daemon=True)
        t.start()

        assert node.coordinator_event.wait(timeout=5.0), (
            "Single node did not elect itself within the timeout"
        )
        assert node.coordinator_id == 73
        assert node.is_coordinator()
        assert node.in_election is False

    # ------------------------------------------------------------------
    # 3. Token forwarding — middle-node behaviour
    # ------------------------------------------------------------------

    def test_token_forwarding(self):
        """The middle node (75) must forward higher tokens and discard lower ones.

        Ring: 74 → 75 → 76 → 74.

        Expected trace:
        - Node 74 sends token 74 to node 75: 74 < 75 → discard (not forwarded).
        - Node 75 sends token 75 to node 76: 75 < 76 → discarded by 76.
        - Node 76 sends token 76 to node 74: 76 > 74 → 74 forwards to 75.
        - Node 75 receives token 76: 76 > 75 → forward to 76 (recorded).
        - Node 76 receives token 76 == 76 → declares coordinator.

        Assertions on node_75.forwarded_cr_elections:
        - 76 must be present (forwarded).
        - 74 must be absent (discarded without forwarding).
        """
        all_ids = [74, 75, 76]
        node_74 = TrackingCRNode(74, all_ids)
        node_75 = TrackingCRNode(75, all_ids)
        node_76 = TrackingCRNode(76, all_ids)
        nodes = [node_74, node_75, node_76]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        _trigger_elections(nodes)

        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Election in token-forwarding test did not complete within the timeout"
        )

        # All nodes must agree on the winner.
        for node in nodes:
            assert node.coordinator_id == 76, (
                f"Node {node.node_id} has wrong coordinator_id: {node.coordinator_id}"
            )

        # Token 76 must have been forwarded by node 75.
        assert 76 in node_75.forwarded_cr_elections, (
            "Node 75 should have forwarded candidate 76 (76 > 75)"
        )

        # Token 74 must have been discarded (not forwarded) by node 75.
        assert 74 not in node_75.forwarded_cr_elections, (
            "Node 75 incorrectly forwarded candidate 74 (should have been discarded: 74 < 75)"
        )

    # ------------------------------------------------------------------
    # 4. Two-node ring
    # ------------------------------------------------------------------

    def test_two_node_ring(self):
        """In a two-node ring, the higher ID (78) must be elected coordinator.

        Ring: 77 → 78 → 77.

        Token 77 reaches node 78 and is discarded (77 < 78).
        Token 78 reaches node 77, is forwarded back to node 78 (78 > 77),
        then 78 == 78 → winner.  Both nodes learn coordinator_id == 78.
        """
        all_ids = [77, 78]
        nodes = [TrackingCRNode(nid, all_ids) for nid in all_ids]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        _trigger_elections(nodes)

        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Two-node election did not complete within the timeout"
        )
        for node in nodes:
            assert node.coordinator_id == 78, (
                f"Node {node.node_id} has wrong coordinator_id: {node.coordinator_id}"
            )

    # ------------------------------------------------------------------
    # 5. Re-election — second election in a fully-alive cluster
    # ------------------------------------------------------------------

    def test_reelection(self):
        """A second election in the same cluster again elects the highest node.

        The Chang-Roberts algorithm does not tolerate node failures: if a node's
        right neighbour is unreachable, the token is lost and the election stalls.
        This test therefore validates re-election in a *fully-alive* cluster,
        confirming that the algorithm is idempotent and produces the same winner
        across multiple rounds.

        Ring: 79 → 80 → 81 → 79.

        Phase 1: initial election → node 81 wins.
        Phase 2: state is reset, all nodes start a new election → node 81 wins again.
        """
        all_ids = [79, 80, 81]
        nodes = [TrackingCRNode(nid, all_ids) for nid in all_ids]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        # --- Phase 1: initial election ----------------------------------------
        _trigger_elections(nodes)
        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Phase-1 election did not complete within the timeout"
        )
        for node in nodes:
            assert node.coordinator_id == 81, (
                f"Phase-1: node {node.node_id} has wrong coordinator_id: {node.coordinator_id}"
            )

        # Allow any in-flight messages to drain before resetting state.
        time.sleep(0.5)

        # --- Phase 2: reset and re-elect --------------------------------------
        for node in nodes:
            node.coordinator_event.clear()
            node.coordinator_id = None
            node.in_election = False
            node.forwarded_cr_elections.clear()

        _trigger_elections(nodes)
        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Phase-2 re-election did not complete within the timeout"
        )
        for node in nodes:
            assert node.coordinator_id == 81, (
                f"Phase-2: node {node.node_id} has wrong coordinator_id after re-election: "
                f"{node.coordinator_id}"
            )

    # ------------------------------------------------------------------
    # 6. is_coordinator() reflects actual state
    # ------------------------------------------------------------------

    def test_is_coordinator_unit(self):
        """is_coordinator() tracks coordinator_id vs node_id correctly.

        Pure unit test — no socket is bound, start() is never called.
        """
        node = ChangRobertsNode(199, [199])

        # Initial state: coordinator_id is None.
        assert not node.is_coordinator()

        # Simulate winning.
        node.coordinator_id = 199
        assert node.is_coordinator()

        # Simulate a different coordinator taking over.
        node.coordinator_id = 200
        assert not node.is_coordinator()

        # Reset to None.
        node.coordinator_id = None
        assert not node.is_coordinator()

    def test_is_coordinator_after_election(self):
        """After a real election the winner returns True; others return False.

        Uses a fresh 2-node ring (IDs 85, 86) so this test is independent
        of the others.  Node 86 must be coordinator; node 85 must not be.
        """
        all_ids = [85, 86]
        nodes = [TrackingCRNode(nid, all_ids) for nid in all_ids]
        self._nodes.extend(nodes)
        _start_nodes(nodes)

        _trigger_elections(nodes)

        assert _all_coordinator_events_set(nodes, timeout=8.0), (
            "Election in is_coordinator test did not complete within the timeout"
        )

        node_85, node_86 = nodes
        assert node_86.is_coordinator(), (
            "Node 86 (highest ID) should report is_coordinator() == True"
        )
        assert not node_85.is_coordinator(), (
            "Node 85 should report is_coordinator() == False"
        )
