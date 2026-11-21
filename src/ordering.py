import sys
import threading

from src.messaging import Message
from src.node import Node


class VectorClock:
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        self._node_id: int = node_id
        self._clock: dict[int, int] = {nid: 0 for nid in all_node_ids}

    def increment(self) -> dict:
        self._clock[self._node_id] += 1
        return dict(self._clock)

    def update(self, received: dict) -> dict:
        for k in self._clock:
            if k in received:
                self._clock[k] = max(self._clock[k], received[k])
        self._clock[self._node_id] += 1
        return dict(self._clock)

    def can_deliver(self, sender_id: int, msg_vc: dict) -> bool:
        expected_from_sender = self._clock.get(sender_id, 0) + 1
        if msg_vc.get(sender_id, 0) != expected_from_sender:
            return False
        for k, v in msg_vc.items():
            if k != sender_id and v > self._clock.get(k, 0):
                return False
        return True

    def to_dict(self) -> dict:
        return dict(self._clock)

    @staticmethod
    def from_dict(d: dict) -> dict:
        return d


class CausalNode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._vc = VectorClock(node_id, all_node_ids)
        self._pending: list[Message] = []
        self._ordering_lock = threading.Lock()

    def broadcast_causal(self, msg_type: str, payload: dict) -> None:
        with self._ordering_lock:
            vc_snapshot = self._vc.increment()

        tagged_payload = dict(payload)
        tagged_payload["vc"] = vc_snapshot
        self.broadcast(msg_type, tagged_payload)

    def handle_message(self, msg: Message) -> None:
        if msg.type != "CAUSAL_MSG":
            super().handle_message(msg)
            return

        to_deliver: list[Message] = []

        with self._ordering_lock:
            msg_vc = {int(k): v for k, v in msg.payload.get("vc", {}).items()}
            if self._vc.can_deliver(msg.sender_id, msg_vc):
                self._vc.update(msg_vc)
                to_deliver.append(msg)
                to_deliver.extend(self._collect_deliverable())
            else:
                self._pending.append(msg)

        for m in to_deliver:
            super().handle_message(m)

    def _try_deliver_pending(self) -> None:
        with self._ordering_lock:
            deliverable = self._collect_deliverable()

        for m in deliverable:
            super().handle_message(m)

    def _collect_deliverable(self) -> list[Message]:
        delivered: list[Message] = []
        progress = True
        while progress:
            progress = False
            remaining: list[Message] = []
            for pm in self._pending:
                pm_vc = {int(k): v for k, v in pm.payload.get("vc", {}).items()}
                if self._vc.can_deliver(pm.sender_id, pm_vc):
                    self._vc.update(pm_vc)
                    delivered.append(pm)
                    progress = True
                else:
                    remaining.append(pm)
            self._pending = remaining
        return delivered


class FIFONode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._send_seq: int = 0
        self._next_expected: dict[int, int] = {}
        self._pending: dict[int, list[Message]] = {}
        self._fifo_lock = threading.Lock()

    def broadcast_fifo(self, msg_type: str, payload: dict) -> None:
        tagged_payload = dict(payload)
        with self._fifo_lock:
            seq = self._send_seq
            self._send_seq += 1
        tagged_payload["fifo_seq"] = seq
        self.broadcast(msg_type, tagged_payload)

    def handle_message(self, msg: Message) -> None:
        if msg.type != "FIFO_MSG":
            super().handle_message(msg)
            return

        with self._fifo_lock:
            to_deliver = self._process_fifo(msg)

        for m in to_deliver:
            super().handle_message(m)

    def _process_fifo(self, msg: Message) -> list[Message]:
        seq: int = msg.payload["fifo_seq"]
        sid: int = msg.sender_id

        if sid not in self._next_expected:
            self._next_expected[sid] = 0
        if sid not in self._pending:
            self._pending[sid] = []

        expected = self._next_expected[sid]

        if seq < expected:
            return []

        if seq > expected:
            self._pending[sid].append(msg)
            return []

        deliverable: list[Message] = [msg]
        self._next_expected[sid] += 1

        deliverable.extend(self._drain_pending(sid))
        return deliverable

    def _drain_pending(self, sender_id: int) -> list[Message]:
        drained: list[Message] = []
        progress = True
        while progress:
            progress = False
            remaining: list[Message] = []
            for pm in self._pending[sender_id]:
                if pm.payload["fifo_seq"] == self._next_expected[sender_id]:
                    drained.append(pm)
                    self._next_expected[sender_id] += 1
                    progress = True
                else:
                    remaining.append(pm)
            self._pending[sender_id] = remaining
        return drained


_SEQUENCER_REQUEST = "SEQUENCER_REQUEST"
_SEQUENCER_DELIVER = "SEQUENCER_DELIVER"


class TotalOrderNode(Node):
    def __init__(
        self, node_id: int, all_node_ids: list[int], sequencer_id: int
    ) -> None:
        super().__init__(node_id, all_node_ids)
        self._sequencer_id = sequencer_id
        self._next_seq: int = 0
        self._hold_back: dict[int, Message] = {}
        self._seq_lock = threading.Lock()
        self._seq_counter: int = 0

    def broadcast_total(self, msg_type: str, payload: dict) -> None:
        self.send(
            self._sequencer_id,
            _SEQUENCER_REQUEST,
            {"original_type": msg_type, "original_payload": payload},
        )

    def handle_message(self, msg: Message) -> None:
        if msg.type == _SEQUENCER_REQUEST:
            if self.node_id == self._sequencer_id:
                self._handle_sequencer_request(msg)
            else:
                super().handle_message(msg)
            return

        if msg.type == _SEQUENCER_DELIVER:
            self._handle_sequencer_deliver(msg)
            return

        super().handle_message(msg)

    def _handle_sequencer_request(self, msg: Message) -> None:
        with self._seq_lock:
            seq = self._seq_counter
            self._seq_counter += 1

        deliver_payload: dict = {
            "seq": seq,
            "original_type": msg.payload["original_type"],
            "original_payload": msg.payload["original_payload"],
        }

        self.broadcast(_SEQUENCER_DELIVER, deliver_payload)

        synthetic = Message(
            type=_SEQUENCER_DELIVER,
            sender_id=self.node_id,
            payload=deliver_payload,
            lamport_clock=self._clock.value,
        )
        self._handle_sequencer_deliver(synthetic)

    def _handle_sequencer_deliver(self, msg: Message) -> None:
        seq: int = msg.payload["seq"]

        to_deliver: list[Message] = []
        with self._seq_lock:
            self._hold_back[seq] = msg
            while self._next_seq in self._hold_back:
                to_deliver.append(self._hold_back.pop(self._next_seq))
                self._next_seq += 1

        for m in to_deliver:
            super().handle_message(m)


def run_fifo() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.ordering fifo <node_id> <all_node_ids>")
        print("Example: python -m src.ordering fifo 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]

    node = FIFONode(node_id, all_node_ids)
    node.start()

    print(
        f"FIFONode {node_id} running. "
        "Type a message and press Enter to broadcast with FIFO ordering. Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line:
                node.broadcast_fifo("FIFO_MSG", {"text": line})
    except (EOFError, KeyboardInterrupt):
        print(f"\nFIFONode {node_id} shutting down.")
        sys.exit(0)


def run_causal() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.ordering causal <node_id> <all_node_ids>")
        print("Example: python -m src.ordering causal 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]

    node = CausalNode(node_id, all_node_ids)
    node.start()

    print(
        f"CausalNode {node_id} running. "
        "Type a message and press Enter to broadcast causally. Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line:
                node.broadcast_causal("CAUSAL_MSG", {"text": line})
    except (EOFError, KeyboardInterrupt):
        print(f"\nCausalNode {node_id} shutting down.")
        sys.exit(0)


def run_total() -> None:
    if len(sys.argv) < 5:
        print(
            "Usage: python -m src.ordering total"
            " <node_id> <all_node_ids> <sequencer_id>"
        )
        print("Example: python -m src.ordering total 1 0,1,2 0")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]
    sequencer_id = int(sys.argv[4])

    node = TotalOrderNode(node_id, all_node_ids, sequencer_id)
    node.start()

    print(
        f"TotalOrderNode {node_id} running (sequencer={sequencer_id}). "
        "Type a message and press Enter to broadcast with total order. Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line:
                node.broadcast_total("USER_MSG", {"text": line})
    except (EOFError, KeyboardInterrupt):
        print(f"\nTotalOrderNode {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("fifo", "causal", "total"):
        print("Usage: python -m src.ordering <fifo|causal|total> ...")
        sys.exit(1)

    if sys.argv[1] == "fifo":
        run_fifo()
    elif sys.argv[1] == "causal":
        run_causal()
    else:
        run_total()
