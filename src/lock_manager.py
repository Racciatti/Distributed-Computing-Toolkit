import logging
import sys
import threading

from src.messaging import Message
from src.node import Node

_LOCK_REQUEST = "LOCK_REQUEST"
_LOCK_GRANTED = "LOCK_GRANTED"
_LOCK_DENIED = "LOCK_DENIED"
_LOCK_RELEASE = "LOCK_RELEASE"
_LOCK_UPGRADE = "LOCK_UPGRADE"

_REQUEST_TIMEOUT = 3.0  # seconds to wait for a manager reply


class LockTable:
    def __init__(self) -> None:
        self._table: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _entry(self, resource: str) -> dict:
        if resource not in self._table:
            self._table[resource] = {"shared": set(), "exclusive": None}
        return self._table[resource]

    def acquire_shared(self, resource: str, tx_id: str) -> bool:
        with self._lock:
            entry = self._entry(resource)
            excl = entry["exclusive"]
            if excl is not None and excl != tx_id:
                return False
            entry["shared"].add(tx_id)
            return True

    def acquire_exclusive(self, resource: str, tx_id: str) -> bool:
        with self._lock:
            entry = self._entry(resource)
            other_shared = entry["shared"] - {tx_id}
            if other_shared:
                return False
            excl = entry["exclusive"]
            if excl is not None and excl != tx_id:
                return False
            entry["shared"].discard(tx_id)
            entry["exclusive"] = tx_id
            return True

    def release(self, tx_id: str) -> None:
        with self._lock:
            for entry in self._table.values():
                entry["shared"].discard(tx_id)
                if entry["exclusive"] == tx_id:
                    entry["exclusive"] = None

    def upgrade(self, resource: str, tx_id: str) -> bool:
        with self._lock:
            entry = self._entry(resource)
            if tx_id not in entry["shared"]:
                return False
            other_shared = entry["shared"] - {tx_id}
            if other_shared:
                return False
            excl = entry["exclusive"]
            if excl is not None and excl != tx_id:
                return False
            entry["shared"].discard(tx_id)
            entry["exclusive"] = tx_id
            return True

    def get_holders(self, resource: str) -> dict:
        with self._lock:
            entry = self._entry(resource)
            return {
                "shared": list(entry["shared"]),
                "exclusive": entry["exclusive"],
            }

    def is_locked(self, resource: str) -> bool:
        with self._lock:
            if resource not in self._table:
                return False
            entry = self._table[resource]
            return bool(entry["shared"]) or entry["exclusive"] is not None


class LockManagerNode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._lock_table = LockTable()

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type == _LOCK_REQUEST:
            self._handle_lock_request(msg)
        elif msg.type == _LOCK_RELEASE:
            self._handle_lock_release(msg)
        elif msg.type == _LOCK_UPGRADE:
            self._handle_lock_upgrade(msg)

    def _handle_lock_request(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        resource = msg.payload.get("resource")
        lock_type = msg.payload.get("lock_type")

        if lock_type == "shared":
            granted = self._lock_table.acquire_shared(resource, tx_id)
        elif lock_type == "exclusive":
            granted = self._lock_table.acquire_exclusive(resource, tx_id)
        else:
            granted = False

        if granted:
            self._log(
                logging.INFO,
                "LOCK_GRANTED tx=%s resource=%s type=%s",
                tx_id,
                resource,
                lock_type,
            )
            self.send(
                msg.sender_id,
                _LOCK_GRANTED,
                {"tx_id": tx_id, "resource": resource},
            )
        else:
            reason = f"Resource '{resource}' is locked; cannot grant {lock_type} lock."
            self._log(
                logging.INFO,
                "LOCK_DENIED tx=%s resource=%s type=%s reason=%s",
                tx_id,
                resource,
                lock_type,
                reason,
            )
            self.send(
                msg.sender_id,
                _LOCK_DENIED,
                {"tx_id": tx_id, "resource": resource, "reason": reason},
            )

    def _handle_lock_release(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        self._lock_table.release(tx_id)
        self._log(logging.INFO, "LOCK_RELEASE tx=%s — all locks released", tx_id)

    def _handle_lock_upgrade(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        resource = msg.payload.get("resource")

        upgraded = self._lock_table.upgrade(resource, tx_id)

        if upgraded:
            self._log(
                logging.INFO,
                "LOCK_UPGRADE granted tx=%s resource=%s",
                tx_id,
                resource,
            )
            self.send(
                msg.sender_id,
                _LOCK_GRANTED,
                {"tx_id": tx_id, "resource": resource},
            )
        else:
            reason = (
                f"Cannot upgrade lock on '{resource}' for tx '{tx_id}'; "
                "tx must be the sole shared holder with no competing exclusive lock."
            )
            self._log(
                logging.INFO,
                "LOCK_UPGRADE denied tx=%s resource=%s reason=%s",
                tx_id,
                resource,
                reason,
            )
            self.send(
                msg.sender_id,
                _LOCK_DENIED,
                {"tx_id": tx_id, "resource": resource, "reason": reason},
            )


class LockClientNode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._pending: dict[tuple, dict] = {}
        self._pending_lock = threading.Lock()

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type in (_LOCK_GRANTED, _LOCK_DENIED):
            self._handle_lock_reply(msg)

    def _handle_lock_reply(self, msg: Message) -> None:
        tx_id = msg.payload.get("tx_id")
        resource = msg.payload.get("resource")
        key = (tx_id, resource)

        with self._pending_lock:
            entry = self._pending.get(key)

        if entry is None:
            return

        entry["granted"] = msg.type == _LOCK_GRANTED
        entry["event"].set()


def request_lock(
    node: Node,
    manager_id: int,
    tx_id: str,
    resource: str,
    lock_type: str,
) -> bool:
    if not isinstance(node, LockClientNode):
        raise TypeError("node must be a LockClientNode to use request_lock.")

    key = (tx_id, resource)
    entry: dict = {"event": threading.Event(), "granted": None}

    with node._pending_lock:
        node._pending[key] = entry

    try:
        node.send(
            manager_id,
            _LOCK_REQUEST,
            {"tx_id": tx_id, "resource": resource, "lock_type": lock_type},
        )
        replied = entry["event"].wait(timeout=_REQUEST_TIMEOUT)
        if not replied:
            return False
        return bool(entry["granted"])
    finally:
        with node._pending_lock:
            node._pending.pop(key, None)


def release_locks(node: Node, manager_id: int, tx_id: str) -> None:
    node.send(manager_id, _LOCK_RELEASE, {"tx_id": tx_id})


def run_manager() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.lock_manager manager <node_id> <all_node_ids>")
        print("Example: python -m src.lock_manager manager 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]

    manager = LockManagerNode(node_id, all_node_ids)
    manager.start()

    print(
        f"Lock manager node {node_id} running. "
        "Waiting for lock requests. Ctrl-C to quit."
    )

    try:
        threading.Event().wait()
    except (EOFError, KeyboardInterrupt):
        print(f"\nLock manager node {node_id} shutting down.")
        sys.exit(0)


def run_client() -> None:
    if len(sys.argv) < 5:
        print(
            "Usage: python -m src.lock_manager client "
            "<node_id> <all_node_ids> <manager_id>"
        )
        print("Example: python -m src.lock_manager client 1 0,1,2 0")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]
    manager_id = int(sys.argv[4])

    client = LockClientNode(node_id, all_node_ids)
    client.start()

    print(
        f"Client node {node_id} running. "
        "Commands: 'lock <tx> <resource> <shared|exclusive>', "
        "'release <tx>', 'upgrade <tx> <resource>'. Ctrl-C to quit."
    )

    try:
        while True:
            line = input("> ").strip()
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd == "lock" and len(parts) == 4:
                tx_id, resource, lock_type = parts[1], parts[2], parts[3]
                if lock_type not in ("shared", "exclusive"):
                    print("lock_type must be 'shared' or 'exclusive'.")
                    continue
                granted = request_lock(client, manager_id, tx_id, resource, lock_type)
                if granted:
                    print(
                        f"Lock GRANTED: tx={tx_id} resource={resource} type={lock_type}"
                    )
                else:
                    print(
                        f"Lock DENIED: tx={tx_id} resource={resource} type={lock_type}"
                    )

            elif cmd == "release" and len(parts) == 2:
                tx_id = parts[1]
                release_locks(client, manager_id, tx_id)
                print(f"LOCK_RELEASE sent for tx={tx_id}")

            elif cmd == "upgrade" and len(parts) == 3:
                tx_id, resource = parts[1], parts[2]
                key = (tx_id, resource)
                entry: dict = {"event": threading.Event(), "granted": None}
                with client._pending_lock:
                    client._pending[key] = entry
                try:
                    client.send(
                        manager_id,
                        _LOCK_UPGRADE,
                        {"tx_id": tx_id, "resource": resource},
                    )
                    replied = entry["event"].wait(timeout=_REQUEST_TIMEOUT)
                    if replied and entry["granted"]:
                        print(f"Upgrade GRANTED: tx={tx_id} resource={resource}")
                    else:
                        print(f"Upgrade DENIED: tx={tx_id} resource={resource}")
                finally:
                    with client._pending_lock:
                        client._pending.pop(key, None)

            else:
                print(
                    "Unknown command or wrong number of arguments. "
                    "Use: lock / release / upgrade"
                )

    except (EOFError, KeyboardInterrupt):
        print(f"\nClient node {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.lock_manager <manager|client> ...")
        sys.exit(1)

    role = sys.argv[1].lower()
    if role == "manager":
        run_manager()
    elif role == "client":
        run_client()
    else:
        print(f"Unknown role: {role!r}. Use 'manager' or 'client'.")
        sys.exit(1)
