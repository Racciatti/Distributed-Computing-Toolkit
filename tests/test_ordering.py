"""Tests for src/ordering.py — VectorClock, CausalNode, TotalOrderNode, FIFONode.

Port allocation
---------------
Causal integration tests       : node IDs 90–92   → ports 5090–5092
Total integration test 8       : node IDs 100–102  → ports 5100–5102
Total integration test 9       : node IDs 103–105  → ports 5103–5105
TotalOrderNode unit test       : node ID 106       → port not bound (no start())
FIFO integration tests         : node IDs 130–132  → ports 5130–5132
FIFO unit tests (no networking): node ID 135, senders 200–201 → not bound
"""

import time

from src.messaging import Message
from src.ordering import (
    CausalNode,
    FIFONode,
    TotalOrderNode,
    VectorClock,
    _SEQUENCER_DELIVER,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _causal_msg(sender_id: int, vc: dict, text: str = "hello") -> Message:
    """Build a CAUSAL_MSG with an embedded vector clock (int keys, no networking)."""
    return Message(
        type="CAUSAL_MSG",
        sender_id=sender_id,
        payload={"text": text, "vc": vc},
        lamport_clock=1,
    )


def _deliver_msg(sender_id: int, seq: int) -> Message:
    """Build a synthetic SEQUENCER_DELIVER message."""
    return Message(
        type=_SEQUENCER_DELIVER,
        sender_id=sender_id,
        payload={
            "seq": seq,
            "original_type": "USER_MSG",
            "original_payload": {"text": f"seq-{seq}"},
        },
        lamport_clock=seq + 1,
    )


def _wait_for(condition, timeout: float = 5.0, poll: float = 0.05) -> bool:
    """Poll *condition* until it returns True or *timeout* expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(poll)
    return False


# ---------------------------------------------------------------------------
# VectorClock unit tests (no networking)
# ---------------------------------------------------------------------------


class TestVectorClock:
    def test_increment_own_entry(self):
        """Fresh clock: increment() sets own entry to 1, others stay 0."""
        vc = VectorClock(node_id=0, all_node_ids=[0, 1, 2])
        snapshot = vc.increment()
        assert snapshot[0] == 1
        assert snapshot[1] == 0
        assert snapshot[2] == 0

    def test_update_pointwise_max(self):
        """update() applies pointwise max then increments own entry."""
        vc = VectorClock(node_id=1, all_node_ids=[0, 1, 2])
        # Local: {0:0, 1:0, 2:0}.  Receive {0:2, 1:1, 2:0}.
        # Expected after update: pointwise max = {0:2, 1:1, 2:0},
        # then increment own (node 1) → {0:2, 1:2, 2:0}.
        received = {0: 2, 1: 1, 2: 0}
        result = vc.update(received)
        assert result[0] == 2
        assert result[1] == 2
        assert result[2] == 0

    def test_can_deliver_true(self):
        """Message is next from sender and has no unseen causal deps → True."""
        vc = VectorClock(node_id=1, all_node_ids=[0, 1, 2])
        # Local: {0:0, 1:0, 2:0}.
        # msg_vc[0]=1 == local[0]+1=1 ✓; msg_vc[1]=0 <= local[1]=0 ✓; msg_vc[2]=0 <= local[2]=0 ✓
        msg_vc = {0: 1, 1: 0, 2: 0}
        assert vc.can_deliver(sender_id=0, msg_vc=msg_vc) is True

    def test_can_deliver_false_not_next(self):
        """Sender's vc entry is 2 but local is 0 — gap (message 1 unseen) → False."""
        vc = VectorClock(node_id=1, all_node_ids=[0, 1, 2])
        # Expected from sender 0 = 0+1 = 1, but msg says 2 → gap.
        msg_vc = {0: 2, 1: 0, 2: 0}
        assert vc.can_deliver(sender_id=0, msg_vc=msg_vc) is False

    def test_can_deliver_false_unseen_dep(self):
        """Sender entry is next, but another node's vc entry is ahead → False."""
        vc = VectorClock(node_id=1, all_node_ids=[0, 1, 2])
        # msg_vc[0]=1 == local[0]+1=1 ✓, but msg_vc[2]=1 > local[2]=0 → unseen dep.
        msg_vc = {0: 1, 1: 0, 2: 1}
        assert vc.can_deliver(sender_id=0, msg_vc=msg_vc) is False


# ---------------------------------------------------------------------------
# CausalNode integration tests (ports 5090–5092)
# ---------------------------------------------------------------------------


class TestCausalNodeIntegration:
    def test_causal_broadcast_delivered(self):
        """3 nodes: node 90 broadcasts a CAUSAL_MSG; nodes 91 and 92 must deliver it."""
        all_ids = [90, 91, 92]
        nodes = {nid: CausalNode(nid, all_ids) for nid in all_ids}
        for n in nodes.values():
            n.start()
        time.sleep(0.3)

        nodes[90].broadcast_causal("CAUSAL_MSG", {"text": "hello"})

        for receiver_id in [91, 92]:
            delivered = _wait_for(
                lambda rid=receiver_id: nodes[rid]._vc.to_dict().get(90, 0) >= 1
            )
            assert delivered, (
                f"Node {receiver_id} did not deliver the causal message from node 90 within 5 s"
            )
            with nodes[receiver_id]._ordering_lock:
                assert nodes[receiver_id]._pending == []

    def test_causal_ordering_respected(self):
        """msg2 (causally after msg1) injected first stays in _pending until msg1 arrives."""
        all_ids = [90, 91, 92]
        # No listener started — messages injected directly via handle_message.
        node = CausalNode(node_id=91, all_node_ids=all_ids)

        # msg1: first message ever from node 90.
        msg1 = _causal_msg(sender_id=90, vc={90: 1, 91: 0, 92: 0}, text="first")
        # msg2: second message from node 90 (causally depends on msg1).
        msg2 = _causal_msg(sender_id=90, vc={90: 2, 91: 0, 92: 0}, text="second")

        # Inject msg2 first — causal delivery condition is not met (msg1 missing).
        node.handle_message(msg2)
        assert len(node._pending) == 1
        assert node._pending[0] is msg2

        # Inject msg1 — satisfies msg2's dependency; both should be delivered.
        node.handle_message(msg1)
        assert node._pending == []


# ---------------------------------------------------------------------------
# TotalOrderNode integration tests (ports 5100–5107)
# ---------------------------------------------------------------------------


class TestTotalOrderNodeIntegration:
    def test_total_broadcast_delivered(self):
        """Node 101 broadcasts via sequencer 100; all 3 nodes deliver with seq=0."""
        all_ids = [100, 101, 102]
        sequencer_id = 100
        nodes = {nid: TotalOrderNode(nid, all_ids, sequencer_id) for nid in all_ids}
        for n in nodes.values():
            n.start()
        time.sleep(0.3)

        nodes[101].broadcast_total("USER_MSG", {"text": "hello"})

        for nid in all_ids:
            done = _wait_for(
                lambda n=nid: nodes[n]._next_seq == 1 and not nodes[n]._hold_back
            )
            with nodes[nid]._seq_lock:
                assert done, (
                    f"Node {nid} did not deliver seq=0; "
                    f"_next_seq={nodes[nid]._next_seq} _hold_back={nodes[nid]._hold_back}"
                )
                assert nodes[nid]._hold_back == {}

    def test_total_ordering_sequential(self):
        """Nodes 103 and 104 each broadcast; all 3 nodes deliver both in seq order."""
        all_ids = [103, 104, 105]
        sequencer_id = 103
        nodes = {nid: TotalOrderNode(nid, all_ids, sequencer_id) for nid in all_ids}
        for n in nodes.values():
            n.start()
        time.sleep(0.3)

        # Sequencer (103) broadcasts first, then node 104.
        nodes[103].broadcast_total("USER_MSG", {"text": "from-103"})
        nodes[104].broadcast_total("USER_MSG", {"text": "from-104"})

        for nid in all_ids:
            done = _wait_for(
                lambda n=nid: nodes[n]._next_seq == 2 and not nodes[n]._hold_back
            )
            with nodes[nid]._seq_lock:
                assert done, (
                    f"Node {nid} did not deliver both messages; "
                    f"_next_seq={nodes[nid]._next_seq} _hold_back={nodes[nid]._hold_back}"
                )
                assert nodes[nid]._hold_back == {}

    def test_hold_back_releases_in_order(self):
        """Inject SEQUENCER_DELIVER seq=1 then seq=0; assert seq=0 delivered first."""
        # Node 106 — listener is NOT started; messages injected directly.
        node = TotalOrderNode(node_id=106, all_node_ids=[106], sequencer_id=106)

        msg_seq1 = _deliver_msg(sender_id=106, seq=1)
        msg_seq0 = _deliver_msg(sender_id=106, seq=0)

        # Inject seq=1 first.  next_seq is 0, so seq=1 must wait in hold_back.
        node._handle_sequencer_deliver(msg_seq1)
        with node._seq_lock:
            assert node._next_seq == 0, (
                "next_seq should still be 0 after injecting seq=1"
            )
            assert 1 in node._hold_back, "seq=1 should be in hold_back"

        # Inject seq=0.  Both 0 and 1 must now flush in order → next_seq == 2.
        node._handle_sequencer_deliver(msg_seq0)
        with node._seq_lock:
            assert node._next_seq == 2, (
                "next_seq should be 2 after both messages delivered"
            )
            assert node._hold_back == {}


# ---------------------------------------------------------------------------
# FIFONode tests (ports 5130–5132 for integration; no ports for unit tests)
# ---------------------------------------------------------------------------


def _fifo_msg(sender_id: int, seq: int, text: str = "hi") -> Message:
    """Build a FIFO_MSG with an embedded fifo_seq (no networking)."""
    return Message(
        type="FIFO_MSG",
        sender_id=sender_id,
        payload={"fifo_seq": seq, "text": text},
        lamport_clock=1,
    )


class TestFIFONode:
    """Tests for FIFONode.

    Integration tests (test_fifo_broadcast_delivered,
    test_fifo_integration_multiple_broadcasts) use real TCP via nodes 130–132
    (ports 5130–5132) started/stopped in setup_method/teardown_method.

    Unit tests (test_fifo_in_order_delivery, test_fifo_out_of_order_buffering,
    test_fifo_duplicate_discarded, test_fifo_multiple_senders_independent)
    inject messages directly via handle_message with no listener started.
    """

    _ALL_IDS = [130, 131, 132]

    def setup_method(self) -> None:
        self._nodes = {nid: FIFONode(nid, self._ALL_IDS) for nid in self._ALL_IDS}
        for n in self._nodes.values():
            n.start()
        time.sleep(0.3)

    def teardown_method(self) -> None:
        for n in self._nodes.values():
            n.stop()
        self._nodes = {}

    # ------------------------------------------------------------------
    # Integration tests
    # ------------------------------------------------------------------

    def test_fifo_broadcast_delivered(self):
        """Node 130 calls broadcast_fifo once; nodes 131 and 132 must deliver it."""
        self._nodes[130].broadcast_fifo("FIFO_MSG", {"text": "hello"})

        for receiver_id in [131, 132]:
            delivered = _wait_for(
                lambda rid=receiver_id: self._nodes[rid]._next_expected.get(130, 0) >= 1
            )
            assert delivered, (
                f"Node {receiver_id} did not deliver the FIFO message from "
                f"node 130 within 5 s"
            )
            with self._nodes[receiver_id]._fifo_lock:
                assert self._nodes[receiver_id]._pending.get(130, []) == []

    def test_fifo_integration_multiple_broadcasts(self):
        """Node 130 calls broadcast_fifo twice; nodes 131 and 132 must show _next_expected[130] == 2."""
        self._nodes[130].broadcast_fifo("FIFO_MSG", {"text": "first"})
        self._nodes[130].broadcast_fifo("FIFO_MSG", {"text": "second"})

        for receiver_id in [131, 132]:
            delivered = _wait_for(
                lambda rid=receiver_id: self._nodes[rid]._next_expected.get(130, 0) >= 2
            )
            assert delivered, (
                f"Node {receiver_id} did not deliver both FIFO messages from "
                f"node 130 within 5 s"
            )
            with self._nodes[receiver_id]._fifo_lock:
                assert self._nodes[receiver_id]._pending.get(130, []) == []

    # ------------------------------------------------------------------
    # Unit tests — no networking, messages injected via handle_message
    # ------------------------------------------------------------------

    def test_fifo_in_order_delivery(self):
        """Inject seq=0 then seq=1 directly; both are delivered in order."""
        node = FIFONode(node_id=135, all_node_ids=[135, 200])
        sender_id = 200

        node.handle_message(_fifo_msg(sender_id=sender_id, seq=0))
        node.handle_message(_fifo_msg(sender_id=sender_id, seq=1))

        with node._fifo_lock:
            assert node._next_expected[sender_id] == 2, (
                "_next_expected should be 2 after delivering seq 0 and 1"
            )
            assert node._pending.get(sender_id, []) == [], (
                "_pending should be empty after both messages delivered"
            )

    def test_fifo_out_of_order_buffering(self):
        """Inject seq=1 before seq=0; seq=1 is buffered, then both delivered on seq=0."""
        node = FIFONode(node_id=135, all_node_ids=[135, 200])
        sender_id = 200

        msg0 = _fifo_msg(sender_id=sender_id, seq=0)
        msg1 = _fifo_msg(sender_id=sender_id, seq=1)

        # Out-of-order: inject seq=1 first.
        node.handle_message(msg1)
        with node._fifo_lock:
            assert node._pending[sender_id] == [msg1], (
                "seq=1 should be buffered in _pending"
            )
            assert node._next_expected[sender_id] == 0, (
                "_next_expected should remain 0 while seq=0 has not arrived"
            )

        # Now inject seq=0; both should be delivered.
        node.handle_message(msg0)
        with node._fifo_lock:
            assert node._next_expected[sender_id] == 2, (
                "_next_expected should be 2 after seq=0 triggers drain of seq=1"
            )
            assert node._pending[sender_id] == [], (
                "_pending should be empty after both messages delivered"
            )

    def test_fifo_duplicate_discarded(self):
        """Delivering seq=0 twice leaves _next_expected at 1 (duplicate silently dropped)."""
        node = FIFONode(node_id=135, all_node_ids=[135, 200])
        sender_id = 200

        msg0 = _fifo_msg(sender_id=sender_id, seq=0)

        node.handle_message(msg0)
        node.handle_message(msg0)  # duplicate — must be silently discarded

        with node._fifo_lock:
            assert node._next_expected[sender_id] == 1, (
                "_next_expected must stay at 1; duplicate must not advance the counter"
            )

    def test_fifo_multiple_senders_independent(self):
        """Per-sender counters are tracked independently."""
        node = FIFONode(node_id=135, all_node_ids=[135, 200, 201])
        sender_a = 200
        sender_b = 201

        # Sender A delivers seq=0 and seq=1.
        node.handle_message(_fifo_msg(sender_id=sender_a, seq=0))
        node.handle_message(_fifo_msg(sender_id=sender_a, seq=1))

        # Sender B delivers only seq=0.
        node.handle_message(_fifo_msg(sender_id=sender_b, seq=0))

        with node._fifo_lock:
            assert node._next_expected[sender_a] == 2, (
                "sender A: _next_expected should be 2 after seq=0 and seq=1"
            )
            assert node._next_expected[sender_b] == 1, (
                "sender B: _next_expected should be 1 after seq=0"
            )
            assert node._pending.get(sender_a, []) == [], (
                "sender A: no pending messages expected"
            )
            assert node._pending.get(sender_b, []) == [], (
                "sender B: no pending messages expected"
            )
