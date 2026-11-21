import json
import logging
import sys
import threading
import uuid

from src.messaging import Message
from src.node import Node
from src.shared_memory import SharedMemory

_VOTE_YES = "VOTE_YES"
_VOTE_NO = "VOTE_NO"
_PREPARE = "PREPARE"
_COMMIT = "COMMIT"
_ABORT = "ABORT"
_ACK = "ACK"

_VOTE_TIMEOUT = 3.0  # seconds to wait for all votes before aborting


class TransactionCoordinator(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._transactions: dict[str, dict] = {}
        self._tx_lock = threading.Lock()

    def run_transaction(
        self, participant_ids: list[int], operations: list[dict]
    ) -> bool:
        tx_id = uuid.uuid4().hex
        n = len(participant_ids)

        entry: dict = {
            "participant_ids": list(participant_ids),
            "votes": {},  # sender_id → "YES" | "NO"
            "acks": set(),
            "vote_event": threading.Event(),
            "ack_event": threading.Event(),
            "decision": None,
        }

        with self._tx_lock:
            self._transactions[tx_id] = entry

        self._log(
            logging.INFO,
            "TX %s — Phase 1 PREPARE → participants=%s ops=%s",
            tx_id,
            participant_ids,
            operations,
        )

        prepare_payload = {"tx_id": tx_id, "operations": operations}
        for pid in participant_ids:
            try:
                self.send(pid, _PREPARE, prepare_payload)
            except OSError:
                self._log(
                    logging.WARNING,
                    "TX %s — could not reach participant %d for PREPARE",
                    tx_id,
                    pid,
                )

        entry["vote_event"].wait(timeout=_VOTE_TIMEOUT)

        with self._tx_lock:
            votes = dict(entry["votes"])

        all_yes = len(votes) == n and all(v == "YES" for v in votes.values())
        decision = _COMMIT if all_yes else _ABORT

        with self._tx_lock:
            entry["decision"] = decision

        self._log(
            logging.INFO,
            "TX %s — votes=%s → decision=%s",
            tx_id,
            votes,
            decision,
        )

        decision_payload = {"tx_id": tx_id}
        for pid in participant_ids:
            try:
                self.send(pid, decision, decision_payload)
            except OSError:
                self._log(
                    logging.WARNING,
                    "TX %s — could not reach participant %d for %s",
                    tx_id,
                    pid,
                    decision,
                )

        entry["ack_event"].wait(timeout=_VOTE_TIMEOUT)

        with self._tx_lock:
            self._transactions.pop(tx_id, None)

        self._log(
            logging.INFO,
            "TX %s — completed (decision=%s, acks=%d/%d)",
            tx_id,
            decision,
            len(entry["acks"]),
            n,
        )

        return decision == _COMMIT

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type in (_VOTE_YES, _VOTE_NO):
            self._handle_vote(msg)
        elif msg.type == _ACK:
            self._handle_ack(msg)

    def _handle_vote(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        vote = "YES" if msg.type == _VOTE_YES else "NO"

        with self._tx_lock:
            entry = self._transactions.get(tx_id)
            if entry is None:
                return
            entry["votes"][msg.sender_id] = vote
            n = len(entry["participant_ids"])
            vote_count = len(entry["votes"])

        self._log(
            logging.DEBUG,
            "TX %s — vote from node %d: %s (%d/%d received)",
            tx_id,
            msg.sender_id,
            vote,
            vote_count,
            n,
        )

        if vote_count >= n or vote == "NO":
            entry["vote_event"].set()

    def _handle_ack(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")

        with self._tx_lock:
            entry = self._transactions.get(tx_id)
            if entry is None:
                return
            entry["acks"].add(msg.sender_id)
            n = len(entry["participant_ids"])
            ack_count = len(entry["acks"])

        self._log(
            logging.DEBUG,
            "TX %s — ACK from node %d (%d/%d received)",
            tx_id,
            msg.sender_id,
            ack_count,
            n,
        )

        if ack_count >= n:
            entry["ack_event"].set()


class TransactionParticipant(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._memory = SharedMemory()
        self._prepared: dict[str, list[dict]] = {}
        self._prepared_lock = threading.Lock()

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type == _PREPARE:
            self._handle_prepare(msg)
        elif msg.type == _COMMIT:
            self._handle_commit(msg)
        elif msg.type == _ABORT:
            self._handle_abort(msg)

    def _validate_operations(self, operations: list[dict]) -> bool:
        for op in operations:
            op_type = op.get("op")
            key = op.get("key")

            if not isinstance(key, str) or not key:
                return False

            if op_type == "set":
                try:
                    json.dumps(op.get("value"))
                except (TypeError, ValueError):
                    return False
            elif op_type == "delete":
                pass
            else:
                return False

        return True

    def _handle_prepare(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        operations = msg.payload.get("operations", [])
        coordinator_id = msg.sender_id

        valid = self._validate_operations(operations)

        self._log(
            logging.INFO,
            "TX %s — PREPARE received from node %d, valid=%s ops=%s",
            tx_id,
            coordinator_id,
            valid,
            operations,
        )

        if valid:
            with self._prepared_lock:
                self._prepared[tx_id] = operations
            self.send(coordinator_id, _VOTE_YES, {"tx_id": tx_id})
        else:
            self.send(coordinator_id, _VOTE_NO, {"tx_id": tx_id})

    def _handle_commit(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        coordinator_id = msg.sender_id

        with self._prepared_lock:
            operations = self._prepared.pop(tx_id, [])

        self._log(
            logging.INFO,
            "TX %s — COMMIT received, applying %d operation(s)",
            tx_id,
            len(operations),
        )

        for op in operations:
            key = op["key"]
            if op["op"] == "set":
                self._memory.set(key, op["value"])
                self._log(logging.DEBUG, "TX %s — set %r = %r", tx_id, key, op["value"])
            elif op["op"] == "delete":
                self._memory.delete(key)
                self._log(logging.DEBUG, "TX %s — deleted %r", tx_id, key)

        self.send(coordinator_id, _ACK, {"tx_id": tx_id})

    def _handle_abort(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        coordinator_id = msg.sender_id

        with self._prepared_lock:
            self._prepared.pop(tx_id, None)

        self._log(
            logging.INFO,
            "TX %s — ABORT received, prepared operations discarded",
            tx_id,
        )

        self.send(coordinator_id, _ACK, {"tx_id": tx_id})


def run_coordinator() -> None:
    if len(sys.argv) < 5:
        print(
            "Usage: python -m src.transactions coordinator "
            "<node_id> <all_node_ids> <participant_ids>"
        )
        print("Example: python -m src.transactions coordinator 0 0,1,2 1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]
    participant_ids = [int(x) for x in sys.argv[4].split(",")]

    coordinator = TransactionCoordinator(node_id, all_node_ids)
    coordinator.start()

    print(
        f"Coordinator node {node_id} running. "
        "Type 'transact' and press Enter to start a transaction. "
        "Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line.strip() == "transact":
                key = input("Key: ").strip()
                raw_value = input("Value: ").strip()
                try:
                    value = json.loads(raw_value)
                except json.JSONDecodeError:
                    # Treat the raw string as a plain string value.
                    value = raw_value

                operations = [{"op": "set", "key": key, "value": value}]
                committed = coordinator.run_transaction(participant_ids, operations)
                if committed:
                    print(f"Transaction COMMITTED: {key} = {value!r}")
                else:
                    print("Transaction ABORTED")
    except (EOFError, KeyboardInterrupt):
        print(f"\nCoordinator node {node_id} shutting down.")
        sys.exit(0)


def run_participant() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.transactions participant <node_id> <all_node_ids>")
        print("Example: python -m src.transactions participant 1 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]

    participant = TransactionParticipant(node_id, all_node_ids)
    participant.start()

    print(
        f"Participant node {node_id} running. "
        "Waiting for transactions from the coordinator. "
        "Ctrl-C to quit."
    )

    try:
        threading.Event().wait()
    except (EOFError, KeyboardInterrupt):
        print(f"\nParticipant node {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.transactions <coordinator|participant> ...")
        sys.exit(1)

    role = sys.argv[1].lower()
    if role == "coordinator":
        run_coordinator()
    elif role == "participant":
        run_participant()
    else:
        print(f"Unknown role: {role!r}. Use 'coordinator' or 'participant'.")
        sys.exit(1)
