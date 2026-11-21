import logging
import sys
import threading

from src.node import Node
from src.messaging import Message


class ElectionNode(Node):
    ELECTION_TIMEOUT: float = 2.0

    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self.coordinator_id: int | None = None
        self.in_election: bool = False
        self._election_lock = threading.Lock()
        self._alive_received = threading.Event()

    def start_election(self) -> None:
        with self._election_lock:
            if self.in_election:
                self._log(logging.DEBUG, "Election already in progress, skipping.")
                return
            self.in_election = True
            self._alive_received.clear()

        self._log(logging.INFO, "Starting election.")

        higher_ids = [nid for nid in self.all_node_ids if nid > self.node_id]

        if not higher_ids:
            # This node has the highest ID — declare victory immediately.
            self._declare_coordinator()
            return

        # Send ELECTION to all nodes with higher IDs.
        for nid in higher_ids:
            try:
                self.send(nid, "ELECTION", {})
            except OSError:
                # Peer may be down; continue trying others.
                self._log(
                    logging.WARNING, "Could not reach node %d during election.", nid
                )

        # Wait for any ALIVE reply.
        got_alive = self._alive_received.wait(timeout=self.ELECTION_TIMEOUT)

        if not got_alive:
            # No higher node responded — this node wins.
            self._declare_coordinator()
        else:
            # A higher node is alive; it will broadcast COORDINATOR when it wins.
            self._log(
                logging.INFO,
                "Received ALIVE from a higher node. Waiting for COORDINATOR.",
            )

    def _declare_coordinator(self) -> None:
        self.coordinator_id = self.node_id
        with self._election_lock:
            self.in_election = False
        self._log(
            logging.INFO, "Declaring self as coordinator (node %d).", self.node_id
        )
        self.broadcast("COORDINATOR", {"coordinator_id": self.node_id})

    def handle_message(self, msg: Message) -> None:
        if msg.type == "ELECTION":
            super().handle_message(msg)
            self._handle_election(msg)
        elif msg.type == "ALIVE":
            super().handle_message(msg)
            self._handle_alive(msg)
        elif msg.type == "COORDINATOR":
            super().handle_message(msg)
            self._handle_coordinator(msg)
        else:
            super().handle_message(msg)

    def _handle_election(self, msg: Message) -> None:
        self._log(
            logging.INFO,
            "Received ELECTION from node %d. Sending ALIVE.",
            msg.sender_id,
        )
        try:
            self.send(msg.sender_id, "ALIVE", {})
        except OSError:
            self._log(
                logging.WARNING,
                "Could not send ALIVE to node %d.",
                msg.sender_id,
            )

        with self._election_lock:
            already = self.in_election

        if not already:
            t = threading.Thread(
                target=self.start_election, daemon=True, name="election"
            )
            t.start()

    def _handle_alive(self, msg: Message) -> None:
        self._log(
            logging.INFO,
            "Received ALIVE from node %d. Backing off.",
            msg.sender_id,
        )
        self._alive_received.set()

    def _handle_coordinator(self, msg: Message) -> None:
        new_coordinator = msg.payload.get("coordinator_id", msg.sender_id)
        self.coordinator_id = new_coordinator
        with self._election_lock:
            self.in_election = False
        self._log(
            logging.INFO,
            "New coordinator is node %d.",
            new_coordinator,
        )

    def is_coordinator(self) -> bool:
        return self.coordinator_id == self.node_id


class ChangRobertsNode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        sorted_ids = sorted(all_node_ids)
        idx = sorted_ids.index(node_id)
        ring_size = len(sorted_ids)
        self.right_neighbor_id: int = sorted_ids[(idx + 1) % ring_size]
        self.left_neighbor_id: int = sorted_ids[(idx - 1) % ring_size]
        self.coordinator_id: int | None = None
        self.in_election: bool = False

    def start_election(self) -> None:
        self.in_election = True
        self._log(
            logging.INFO,
            "Starting CR election. Sending candidate %d to right neighbour %d.",
            self.node_id,
            self.right_neighbor_id,
        )
        try:
            self.send(
                self.right_neighbor_id,
                "CR_ELECTION",
                {"candidate_id": self.node_id},
            )
        except OSError:
            self._log(
                logging.WARNING,
                "Could not reach right neighbour %d during election start.",
                self.right_neighbor_id,
            )

    def _handle_cr_election(self, msg: Message) -> None:
        candidate_id: int = msg.payload["candidate_id"]

        if candidate_id > self.node_id:
            self._log(
                logging.INFO,
                "CR_ELECTION: candidate %d > own %d — forwarding to %d.",
                candidate_id,
                self.node_id,
                self.right_neighbor_id,
            )
            try:
                self.send(
                    self.right_neighbor_id,
                    "CR_ELECTION",
                    {"candidate_id": candidate_id},
                )
            except OSError:
                self._log(
                    logging.WARNING,
                    "Could not forward CR_ELECTION to right neighbour %d.",
                    self.right_neighbor_id,
                )

        elif candidate_id < self.node_id:
            self._log(
                logging.INFO,
                "CR_ELECTION: candidate %d < own %d — discarding.",
                candidate_id,
                self.node_id,
            )

        else:
            # candidate_id == self.node_id: this node is the coordinator.
            self._log(
                logging.INFO,
                "CR_ELECTION: own token returned — declaring self coordinator (node %d).",
                self.node_id,
            )
            self.coordinator_id = self.node_id
            self.in_election = False
            self.broadcast("CR_COORDINATOR", {"coordinator_id": self.node_id})

    def _handle_cr_coordinator(self, msg: Message) -> None:
        new_coordinator: int = msg.payload.get("coordinator_id", msg.sender_id)
        self.coordinator_id = new_coordinator
        self.in_election = False
        self._log(
            logging.INFO,
            "CR_COORDINATOR received. New coordinator is node %d.",
            new_coordinator,
        )

    def handle_message(self, msg: Message) -> None:
        if msg.type == "CR_ELECTION":
            super().handle_message(msg)
            self._handle_cr_election(msg)
        elif msg.type == "CR_COORDINATOR":
            super().handle_message(msg)
            self._handle_cr_coordinator(msg)
        else:
            super().handle_message(msg)

    def is_coordinator(self) -> bool:
        return self.coordinator_id == self.node_id


def run() -> None:
    if len(sys.argv) < 3:
        print("Usage: python -m src.election <node_id> <all_node_ids>")
        print("Example: python -m src.election 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[1])
    all_node_ids = [int(x) for x in sys.argv[2].split(",")]

    node = ElectionNode(node_id, all_node_ids)
    node.start()

    print(
        f"ElectionNode {node_id} running. Type 'elect' to start an election. Ctrl-C to quit."
    )

    t = threading.Thread(target=node.start_election, daemon=True, name="election-init")
    t.start()

    try:
        while True:
            line = input()
            if line.strip().lower() == "elect":
                t = threading.Thread(
                    target=node.start_election, daemon=True, name="election-manual"
                )
                t.start()
    except (EOFError, KeyboardInterrupt):
        print(f"\nNode {node_id} shutting down.")
        sys.exit(0)


def run_cr() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.election cr <node_id> <all_node_ids>")
        print("Example: python -m src.election cr 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]

    node = ChangRobertsNode(node_id, all_node_ids)
    node.start()

    print(
        f"ChangRobertsNode {node_id} running. "
        f"Ring order (sorted): {sorted(all_node_ids)}. "
        f"Right neighbour: {node.right_neighbor_id}. "
        "Type 'elect' to start an election. Ctrl-C to quit."
    )

    t = threading.Thread(
        target=node.start_election, daemon=True, name="cr-election-init"
    )
    t.start()

    try:
        while True:
            line = input()
            if line.strip().lower() == "elect":
                t = threading.Thread(
                    target=node.start_election,
                    daemon=True,
                    name="cr-election-manual",
                )
                t.start()
    except (EOFError, KeyboardInterrupt):
        print(f"\nNode {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "cr":
        run_cr()
    elif len(sys.argv) > 1 and sys.argv[1] == "bully":
        # Support explicit "bully" prefix by shifting the positional args.
        sys.argv.pop(1)
        run()
    else:
        run()
