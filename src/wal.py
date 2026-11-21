import json
import logging
import os
import sys
import threading
import time

from src.messaging import Message
from src.shared_memory import SharedMemory
from src.transactions import TransactionParticipant

_PREPARE = "PREPARE"
_COMMIT = "COMMIT"
_ABORT = "ABORT"
_VOTE_YES = "VOTE_YES"
_VOTE_NO = "VOTE_NO"
_ACK = "ACK"

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class WriteAheadLog:
    def __init__(self, node_id: int, log_dir: str = "logs") -> None:
        self.node_id = node_id
        abs_log_dir = os.path.join(_REPO_ROOT, log_dir)
        os.makedirs(abs_log_dir, exist_ok=True)
        self.log_path = os.path.join(abs_log_dir, f"wal_{node_id}.log")
        self._lock = threading.Lock()

    def log_prepare(self, tx_id: str, operations: list[dict]) -> None:
        entry = {
            "tx_id": tx_id,
            "phase": "prepare",
            "operations": operations,
            "timestamp": time.time(),
        }
        self._append(entry)

    def log_commit(self, tx_id: str) -> None:
        entry = {"tx_id": tx_id, "phase": "commit", "timestamp": time.time()}
        self._append(entry)

    def log_abort(self, tx_id: str) -> None:
        entry = {"tx_id": tx_id, "phase": "abort", "timestamp": time.time()}
        self._append(entry)

    def recover(self) -> list[dict]:
        with self._lock:
            if not os.path.exists(self.log_path):
                return []

            prepared: dict[str, list[dict]] = {}
            decided: set[str] = set()

            try:
                with open(self.log_path, "r", encoding="utf-8") as fh:
                    for raw_line in fh:
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        tx_id = entry.get("tx_id")
                        phase = entry.get("phase")

                        if tx_id is None or phase is None:
                            continue

                        if phase == "prepare":
                            prepared[tx_id] = entry.get("operations", [])
                        elif phase in ("commit", "abort"):
                            decided.add(tx_id)
            except OSError:
                return []

            incomplete = [
                {"tx_id": tx_id, "operations": ops}
                for tx_id, ops in prepared.items()
                if tx_id not in decided
            ]
            return incomplete

    def clear(self) -> None:
        with self._lock:
            try:
                os.remove(self.log_path)
            except FileNotFoundError:
                pass

    def _append(self, entry: dict) -> None:
        line = json.dumps(entry, separators=(",", ":")) + "\n"
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
                os.fsync(fh.fileno())


class RecoverableParticipant(TransactionParticipant):
    def __init__(
        self,
        node_id: int,
        all_node_ids: list[int],
        shared_memory_path: str = "shared_data.txt",
    ) -> None:
        super().__init__(node_id, all_node_ids)
        self._memory = SharedMemory(shared_memory_path)
        self._wal = WriteAheadLog(node_id)
        self.recover_on_startup()

    def recover_on_startup(self) -> None:
        incomplete = self._wal.recover()
        if not incomplete:
            self._log(logging.INFO, "WAL recovery: no incomplete transactions found.")
            return

        for record in incomplete:
            tx_id = record["tx_id"]
            self._log(
                logging.WARNING,
                "WAL recovery: aborting incomplete TX %s (ops=%s)",
                tx_id,
                record["operations"],
            )
            self._wal.log_abort(tx_id)
            with self._prepared_lock:
                self._prepared.pop(tx_id, None)

        self._log(
            logging.INFO,
            "WAL recovery: aborted %d incomplete transaction(s).",
            len(incomplete),
        )

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
            self._wal.log_prepare(tx_id, operations)
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

        self._wal.log_commit(tx_id)

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

        self._wal.log_abort(tx_id)

        self.send(coordinator_id, _ACK, {"tx_id": tx_id})


def run_recoverable_participant() -> None:
    if len(sys.argv) < 4:
        print(
            "Usage: python -m src.wal wal_participant <node_id> <all_node_ids> "
            "[shared_memory_path]"
        )
        print("Example: python -m src.wal wal_participant 1 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]
    shared_memory_path = sys.argv[4] if len(sys.argv) >= 5 else "shared_data.txt"

    participant = RecoverableParticipant(node_id, all_node_ids, shared_memory_path)
    participant.start()

    print(
        f"RecoverableParticipant node {node_id} running. "
        "Waiting for transactions from the coordinator. "
        "Ctrl-C to quit."
    )

    try:
        threading.Event().wait()
    except (EOFError, KeyboardInterrupt):
        print(f"\nRecoverableParticipant node {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.wal <wal_participant> ...")
        sys.exit(1)

    role = sys.argv[1].lower()
    if role == "wal_participant":
        run_recoverable_participant()
    else:
        print(f"Unknown role: {role!r}. Use 'wal_participant'.")
        sys.exit(1)
