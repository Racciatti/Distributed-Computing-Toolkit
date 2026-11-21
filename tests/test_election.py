"""Tests for the Bully (Valentão) leader election algorithm (ElectionNode).

Port / node-ID allocation (port = 5000 + node_id):
    test_highest_node_wins                  : IDs 20, 21, 22  →  ports 5020-5022
    test_single_node_elects_itself          : ID  23           →  port  5023
    test_election_triggers_alive_reply      : IDs 24, 25       →  ports 5024-5025
    test_coordinator_message_sets_state     : ID  99 (no socket — start() not called)
    test_reelection_after_coordinator_loss  : IDs 26, 27, 28   →  ports 5026-5028
    test_lower_id_defers_when_alive_received: ID  29           →  port  5029
"""

import threading
import time

from src.election import ElectionNode
from src.messaging import Message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TrackingElectionNode(ElectionNode):
    """ElectionNode that fires an event the moment a coordinator is determined.

    The event is set in both paths:
    - ``_declare_coordinator``  (this node wins)
    - ``_handle_coordinator``   (another node won and broadcast the result)

    ``_disabled`` can be set to True to make the node silently drop all
    incoming messages, simulating a crashed process.  The flag is checked
    inside the overridden ``handle_message`` so that it is evaluated on
    every call through the bound method already captured by the listener
    thread — plain monkey-patching the attribute after ``start()`` would
    not work because the listener holds a reference to the original bound
    method object.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.coordinator_event = threading.Event()
        self._disabled = False

    def handle_message(self, msg):
        if self._disabled:
            return
        super().handle_message(msg)

    def _declare_coordinator(self):
        super()._declare_coordinator()
        self.coordinator_event.set()

    def _handle_coordinator(self, msg):
        super()._handle_coordinator(msg)
        self.coordinator_event.set()


def _start_nodes(nodes):
    """Call start() on every node and wait for sockets to bind."""
    for node in nodes:
        node.start()
    time.sleep(0.3)


def _wait_all_quiesced(nodes, timeout=8.0):
    """Block until every node has in_election=False and then add a small buffer.

    The extra 0.5 s after the flag check lets any final in-flight TCP
    messages (e.g., late COORDINATOR broadcasts) be delivered before the
    caller mutates node state or inspects coordinator_id.

    Returns True if all nodes settled within *timeout*, False otherwise.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(not node.in_election for node in nodes):
            time.sleep(0.5)  # buffer for last in-flight messages
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBullyElection:
    """Full Bully election algorithm integration and unit tests."""

    # ------------------------------------------------------------------
    # 1. Highest node wins
    # ------------------------------------------------------------------

    def test_highest_node_wins(self):
        """Election triggered from the lowest node must crown the highest node.

        Topology: IDs 20, 21, 22  (ports 5020-5022).
        Election started from node 20.
        Expected: all nodes agree coordinator_id == 22.
        """
        all_ids = [20, 21, 22]
        nodes = [TrackingElectionNode(nid, all_ids) for nid in all_ids]
        _start_nodes(nodes)

        t = threading.Thread(target=nodes[0].start_election, daemon=True)
        t.start()

        # Allow up to 8 s: 2 s for ELECTION_TIMEOUT + propagation slack.
        for node in nodes:
            assert node.coordinator_event.wait(timeout=8.0), (
                f"Node {node.node_id} did not learn the coordinator within timeout"
            )

        for node in nodes:
            assert node.coordinator_id == 22, (
                f"Node {node.node_id} has wrong coordinator: {node.coordinator_id}"
            )

    # ------------------------------------------------------------------
    # 2. Single node elects itself
    # ------------------------------------------------------------------

    def test_single_node_elects_itself(self):
        """A node with no peers in the cluster must declare itself coordinator.

        Node 23 has all_node_ids=[23] — no higher IDs exist.
        start_election() must return synchronously after self-declaration.
        """
        node = TrackingElectionNode(23, [23])
        node.start()
        time.sleep(0.2)

        # With no higher peers this call is synchronous.
        node.start_election()

        assert node.coordinator_id == 23
        assert node.is_coordinator()
        assert node.in_election is False

    # ------------------------------------------------------------------
    # 3. ELECTION message triggers ALIVE reply
    # ------------------------------------------------------------------

    def test_election_triggers_alive_reply(self):
        """When node 24 sends ELECTION to node 25, node 25 must reply ALIVE.

        The reply is confirmed indirectly: node 24's _alive_received Event is
        set only when handle_alive() is called, which happens exclusively on
        receipt of an ALIVE message from a higher node.
        """
        all_ids = [24, 25]
        node_24 = ElectionNode(24, all_ids)
        node_25 = ElectionNode(25, all_ids)
        _start_nodes([node_24, node_25])

        t = threading.Thread(target=node_24.start_election, daemon=True)
        t.start()

        # _alive_received is set iff node 25 sent ALIVE back to node 24.
        assert node_24._alive_received.wait(timeout=5.0), (
            "Node 24 did not receive an ALIVE reply from node 25"
        )

    # ------------------------------------------------------------------
    # 4. COORDINATOR message sets coordinator state
    # ------------------------------------------------------------------

    def test_coordinator_message_sets_state(self):
        """_handle_coordinator must update coordinator_id and clear in_election.

        This is a pure unit test — no network I/O.  The node is created but
        start() is never called, so no socket is bound.
        """
        node = ElectionNode(99, [99])
        # Simulate an in-progress election.
        node.in_election = True

        fake_coordinator_msg = Message(
            type="COORDINATOR",
            sender_id=9,
            payload={"coordinator_id": 9},
            lamport_clock=1,
        )
        node._handle_coordinator(fake_coordinator_msg)

        assert node.coordinator_id == 9
        assert node.in_election is False

    def test_coordinator_message_falls_back_to_sender_id(self):
        """If payload omits coordinator_id, sender_id is used as the coordinator."""
        node = ElectionNode(99, [99])

        fake_msg = Message(
            type="COORDINATOR",
            sender_id=7,
            payload={},  # no coordinator_id key
            lamport_clock=1,
        )
        node._handle_coordinator(fake_msg)

        assert node.coordinator_id == 7

    # ------------------------------------------------------------------
    # 5. Re-election after coordinator loss
    # ------------------------------------------------------------------

    def test_reelection_after_coordinator_loss(self):
        """After the initial coordinator (node 28) goes silent, node 27 must win.

        Phase 1: nodes 26, 27, 28 — initial election from 26 → node 28 wins.
        Phase 2: node 28 is silenced; node 27 triggers re-election → node 27 wins.
        """
        all_ids = [26, 27, 28]
        nodes = {nid: TrackingElectionNode(nid, all_ids) for nid in all_ids}
        _start_nodes(list(nodes.values()))

        # ---- Phase 1: initial election ----------------------------------------
        t1 = threading.Thread(target=nodes[26].start_election, daemon=True)
        t1.start()

        for nid, node in nodes.items():
            assert node.coordinator_event.wait(timeout=8.0), (
                f"Node {nid} did not learn the coordinator in phase 1"
            )

        assert nodes[26].coordinator_id == 28
        assert nodes[27].coordinator_id == 28
        assert nodes[28].coordinator_id == 28

        # ---- Wait for full quiescence before starting phase 2 -----------------
        # The Bully algorithm can generate a chain of elections (node 28 re-enters
        # its own election when it receives a late ELECTION from node 27 after
        # having already declared coordinator).  Each round produces additional
        # COORDINATOR broadcasts that would overwrite phase-2 results if they
        # arrive after we clear coordinator_event.  Polling until in_election is
        # False on all nodes ensures every election thread has finished; the 0.5 s
        # buffer inside _wait_all_quiesced lets the last in-flight TCP messages land.
        assert _wait_all_quiesced(nodes.values()), (
            "Nodes did not fully quiesce after phase 1 election"
        )

        # ---- Simulate node 28 failure ------------------------------------------
        # Set the _disabled flag so TrackingElectionNode.handle_message silently
        # drops all future messages, exactly as if the process had crashed.
        # (Plain monkey-patching the attribute would not work here: the listener
        # thread captured the bound method at start() time, so it always calls
        # the overridden handle_message; _disabled is checked at call time.)
        nodes[28]._disabled = True

        # Reset coordinator events on the surviving nodes so we can detect phase 2.
        nodes[26].coordinator_event.clear()
        nodes[27].coordinator_event.clear()

        # All election threads are done (guaranteed by _wait_all_quiesced above).
        assert nodes[26].in_election is False
        assert nodes[27].in_election is False

        # ---- Phase 2: re-election from node 27 --------------------------------
        t2 = threading.Thread(target=nodes[27].start_election, daemon=True)
        t2.start()

        # Node 27 waits ELECTION_TIMEOUT (2 s) for ALIVE that never comes,
        # then declares itself coordinator and broadcasts to 26 and 28.
        assert nodes[27].coordinator_event.wait(timeout=8.0), (
            "Node 27 did not become coordinator in phase 2"
        )
        assert nodes[26].coordinator_event.wait(timeout=8.0), (
            "Node 26 did not receive the new COORDINATOR broadcast"
        )

        assert nodes[27].coordinator_id == 27
        assert nodes[26].coordinator_id == 27

    # ------------------------------------------------------------------
    # 6. Lower-ID node defers when ALIVE is received
    # ------------------------------------------------------------------

    def test_lower_id_defers_when_alive_received(self):
        """A node that receives ALIVE must not declare itself coordinator.

        Node 29 starts an election targeting node 30 (unreachable).
        Before the 2-second timeout expires, an ALIVE is injected directly
        via _handle_alive() to simulate a higher node replying in time.
        The election thread must finish without calling _declare_coordinator,
        leaving coordinator_id as None.
        """
        node = TrackingElectionNode(29, [29, 30])  # node 30 does not exist
        node.start()
        time.sleep(0.2)

        t = threading.Thread(target=node.start_election, daemon=True)
        t.start()

        # Give start_election time to clear _alive_received and enter .wait().
        # The send to node 30 will fail instantly (OSError), so the thread
        # reaches _alive_received.wait() within a few milliseconds.
        time.sleep(0.2)

        # Inject ALIVE from the "higher" (but actually absent) node 30.
        fake_alive = Message(
            type="ALIVE",
            sender_id=30,
            payload={},
            lamport_clock=1,
        )
        node._handle_alive(fake_alive)

        # Election thread must wake up and exit quickly.
        t.join(timeout=5.0)
        assert not t.is_alive(), "start_election thread did not finish after ALIVE"

        # Node 29 must NOT have set itself as coordinator.
        assert node.coordinator_id is None, (
            "Node 29 incorrectly declared itself coordinator after receiving ALIVE"
        )
        assert not node.is_coordinator()
        # coordinator_event must not have been fired either.
        assert not node.coordinator_event.is_set()
