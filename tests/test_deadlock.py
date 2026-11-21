"""Tests for src/deadlock.py — WaitForGraph and DeadlockDetector.

Port / node-ID allocation (port = 5000 + node_id):
    test_wait_for_message_adds_edge          : ID 60  → port 5060
    test_release_removes_edges               : ID 61  → port 5061
    test_deadlock_detected_broadcast         : IDs 62, 63 → ports 5062, 5063
    test_victim_edges_removed_after_deadlock : ID 64  → port 5064
"""

import threading
import time

from src.deadlock import DeadlockDetector, WaitForGraph
from src.messaging import Message, send_message, start_listener


# ---------------------------------------------------------------------------
# WaitForGraph unit tests (no networking)
# ---------------------------------------------------------------------------


class TestWaitForGraph:
    def test_no_cycle_empty(self):
        """Empty graph must report no cycle."""
        wfg = WaitForGraph()
        assert wfg.has_cycle() is False

    def test_no_cycle_chain(self):
        """Linear chain T1→T2→T3 has no back edge and therefore no cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T2", "T3")
        assert wfg.has_cycle() is False

    def test_simple_cycle(self):
        """T1→T2 and T2→T1 form a 2-node cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T2", "T1")
        assert wfg.has_cycle() is True
        cycles = wfg.find_cycles()
        assert len(cycles) >= 1
        all_cycle_nodes = {n for cycle in cycles for n in cycle}
        assert "T1" in all_cycle_nodes
        assert "T2" in all_cycle_nodes

    def test_three_node_cycle(self):
        """T1→T2→T3→T1 is a 3-node cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T2", "T3")
        wfg.add_edge("T3", "T1")
        assert wfg.has_cycle() is True
        cycles = wfg.find_cycles()
        assert len(cycles) >= 1
        all_cycle_nodes = {n for cycle in cycles for n in cycle}
        assert "T1" in all_cycle_nodes
        assert "T2" in all_cycle_nodes
        assert "T3" in all_cycle_nodes

    def test_no_cycle_dag(self):
        """T1→T3 and T2→T3 (converging DAG) has no cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T3")
        wfg.add_edge("T2", "T3")
        assert wfg.has_cycle() is False

    def test_remove_breaks_cycle(self):
        """Removing T1's edges resolves the T1↔T2 cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T2", "T1")
        assert wfg.has_cycle() is True
        wfg.remove_edges_for("T1")
        assert wfg.has_cycle() is False

    def test_multiple_cycles(self):
        """Two disjoint cycles (T1↔T2 and T3↔T4) produce exactly 2 cycle lists."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T2", "T1")
        wfg.add_edge("T3", "T4")
        wfg.add_edge("T4", "T3")
        cycles = wfg.find_cycles()
        assert len(cycles) == 2

    def test_self_loop(self):
        """A self-loop T1→T1 is a trivial cycle."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T1")
        assert wfg.has_cycle() is True

    def test_get_graph_snapshot(self):
        """get_graph() returns a correct adjacency-list snapshot."""
        wfg = WaitForGraph()
        wfg.add_edge("T1", "T2")
        wfg.add_edge("T1", "T3")
        graph = wfg.get_graph()
        assert "T1" in graph
        assert "T2" in graph
        assert "T3" in graph
        assert "T2" in graph["T1"]
        assert "T3" in graph["T1"]
        # Mutations to the returned dict must not alter internal state.
        graph["T1"].clear()
        assert "T2" in wfg.get_graph()["T1"]


# ---------------------------------------------------------------------------
# DeadlockDetector integration tests
# ---------------------------------------------------------------------------


class TestDeadlockDetectorIntegration:
    def setup_method(self):
        """Initialise per-test node and raw-socket registries."""
        self._nodes = []
        self._raw_socks = []

    def teardown_method(self, method):
        """Stop all nodes and close any raw server sockets created by this test."""
        for n in self._nodes:
            n.stop()
        self._nodes = []
        for s in self._raw_socks:
            try:
                s.close()
            except OSError:
                pass
        self._raw_socks = []
        import time

        time.sleep(0.1)  # allow OS to release ports

    def test_wait_for_message_adds_edge(self):
        """A WAIT_FOR message causes the edge to appear in get_graph()."""
        detector = DeadlockDetector(60, [60])
        self._nodes.append(detector)
        detector.start()
        time.sleep(0.3)

        send_message(
            60,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T1", "holder": "T2"},
                lamport_clock=1,
            ),
        )
        time.sleep(0.3)

        graph = detector._wfg.get_graph()
        assert "T1" in graph, "T1 (waiter) should be a node in the graph"
        assert "T2" in graph["T1"], "T1 should have T2 as a dependent"

    def test_release_removes_edges(self):
        """After a RELEASE message the tx_id's edges are purged from get_graph()."""
        detector = DeadlockDetector(61, [61])
        self._nodes.append(detector)
        detector.start()
        time.sleep(0.3)

        send_message(
            61,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T1", "holder": "T2"},
                lamport_clock=1,
            ),
        )
        time.sleep(0.3)

        graph = detector._wfg.get_graph()
        assert "T1" in graph, "Edge should exist before release"

        send_message(
            61,
            Message(
                type="RELEASE",
                sender_id=99,
                payload={"tx_id": "T1"},
                lamport_clock=2,
            ),
        )
        time.sleep(0.3)

        graph = detector._wfg.get_graph()
        assert "T1" not in graph, "T1 should be fully removed after RELEASE"
        if "T2" in graph:
            assert "T1" not in graph["T2"], "T2 must not reference the released T1"

    def test_deadlock_detected_broadcast(self):
        """Cycle T1→T2→T1 causes DEADLOCK_DETECTED to arrive at peer node 63."""
        received: list[Message] = []
        detected_event = threading.Event()

        def on_message(msg: Message) -> None:
            if msg.type == "DEADLOCK_DETECTED":
                received.append(msg)
                detected_event.set()

        # Observer node (63) must be up before detector tries to broadcast.
        _, srv_63 = start_listener(63, on_message)
        self._raw_socks.append(srv_63)
        detector = DeadlockDetector(62, [62, 63])
        self._nodes.append(detector)
        detector.start()
        time.sleep(0.3)

        # First edge: T1 waits for T2
        send_message(
            62,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T1", "holder": "T2"},
                lamport_clock=1,
            ),
        )
        time.sleep(0.1)

        # Second edge: T2 waits for T1 — completes the cycle
        send_message(
            62,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T2", "holder": "T1"},
                lamport_clock=2,
            ),
        )

        timed_out = not detected_event.wait(timeout=5.0)
        assert not timed_out, (
            "Timed out waiting for DEADLOCK_DETECTED at observer node 63"
        )
        assert len(received) >= 1

        payload = received[0].payload
        assert "cycles" in payload, "DEADLOCK_DETECTED payload must contain 'cycles'"
        assert "victim" in payload, "DEADLOCK_DETECTED payload must contain 'victim'"

        all_cycle_nodes = {n for cycle in payload["cycles"] for n in cycle}
        assert "T1" in all_cycle_nodes, "T1 must be part of the reported cycle"
        assert "T2" in all_cycle_nodes, "T2 must be part of the reported cycle"

    def test_victim_edges_removed_after_deadlock(self):
        """After deadlock resolution the victim's edges are gone from the WaitForGraph."""
        # Detector has no peers; broadcast silently fails — that is intentional.
        detector = DeadlockDetector(64, [64])
        self._nodes.append(detector)
        detector.start()
        time.sleep(0.3)

        # Build cycle T1→T2→T1; victim = max("T1", "T2") = "T2"
        send_message(
            64,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T1", "holder": "T2"},
                lamport_clock=1,
            ),
        )
        time.sleep(0.1)
        send_message(
            64,
            Message(
                type="WAIT_FOR",
                sender_id=99,
                payload={"waiter": "T2", "holder": "T1"},
                lamport_clock=2,
            ),
        )
        time.sleep(0.5)

        graph = detector._wfg.get_graph()
        assert "T2" not in graph, "Victim T2 should be removed from the graph entirely"
        if "T1" in graph:
            assert "T2" not in graph["T1"], (
                "T1 must not retain an edge to the removed victim T2"
            )
