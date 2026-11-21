import logging
import sys
import threading

from src.messaging import Message
from src.node import Node

_WAIT_FOR = "WAIT_FOR"
_RELEASE = "RELEASE"
_DEADLOCK_DETECTED = "DEADLOCK_DETECTED"


class WaitForGraph:
    def __init__(self) -> None:
        self._graph: dict[str, list[str]] = {}
        self._lock = threading.Lock()

    def add_edge(self, waiter: str, holder: str) -> None:
        with self._lock:
            if waiter not in self._graph:
                self._graph[waiter] = []
            if holder not in self._graph:
                self._graph[holder] = []
            if holder not in self._graph[waiter]:
                self._graph[waiter].append(holder)

    def remove_edges_for(self, tx_id: str) -> None:
        with self._lock:
            self._graph.pop(tx_id, None)
            for node in self._graph:
                self._graph[node] = [n for n in self._graph[node] if n != tx_id]

    def has_cycle(self) -> bool:
        with self._lock:
            graph = {k: list(v) for k, v in self._graph.items()}

        color: dict[str, int] = {n: 0 for n in graph}

        def _dfs(node: str) -> bool:
            color[node] = 1  # grey — on active path
            for neighbor in graph.get(node, []):
                if neighbor not in color:
                    color[neighbor] = 0
                if color[neighbor] == 1:
                    return True  # back edge → cycle
                if color[neighbor] == 0 and _dfs(neighbor):
                    return True
            color[node] = 2  # black — fully processed
            return False

        for node in graph:
            if color.get(node, 0) == 0:
                if _dfs(node):
                    return True
        return False

    def find_cycles(self) -> list[list[str]]:
        with self._lock:
            graph = {k: list(v) for k, v in self._graph.items()}

        color: dict[str, int] = {n: 0 for n in graph}
        path: list[str] = []
        cycles: list[list[str]] = []

        def _dfs(node: str) -> None:
            color[node] = 1  # grey — on active path
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in color:
                    color[neighbor] = 0
                if color[neighbor] == 1:
                    # Back edge found — extract the cycle from the path.
                    start_idx = path.index(neighbor)
                    cycles.append(path[start_idx:] + [neighbor])
                elif color[neighbor] == 0:
                    _dfs(neighbor)

            path.pop()
            color[node] = 2  # black — fully processed

        for node in graph:
            if color.get(node, 0) == 0:
                _dfs(node)

        return cycles

    def get_graph(self) -> dict[str, list[str]]:
        with self._lock:
            return {k: list(v) for k, v in self._graph.items()}


class DeadlockDetector(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)
        self._wfg = WaitForGraph()
        self._wfg_lock = threading.Lock()

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type == _WAIT_FOR:
            self._handle_wait_for(msg)
        elif msg.type == _RELEASE:
            self._handle_release(msg)

    def _handle_wait_for(self, msg: Message) -> None:
        waiter: str | None = msg.payload.get("waiter")
        holder: str | None = msg.payload.get("holder")

        if not waiter or not holder:
            self._log(
                logging.WARNING,
                "WAIT_FOR message missing waiter/holder: %s",
                msg.payload,
            )
            return

        cycles: list[list[str]] = []
        victim: str | None = None
        graph_snapshot: dict[str, list[str]] = {}

        with self._wfg_lock:
            self._wfg.add_edge(waiter, holder)
            cycles = self._wfg.find_cycles()

            if cycles:
                all_cycle_nodes: set[str] = {
                    node for cycle in cycles for node in cycle[:-1]
                }
                victim = max(all_cycle_nodes)
                self._wfg.remove_edges_for(victim)

            graph_snapshot = self._wfg.get_graph()

        self._log(
            logging.INFO,
            "WAIT_FOR: %s -> %s | graph=%s",
            waiter,
            holder,
            graph_snapshot,
        )

        if cycles and victim is not None:
            self._log(
                logging.WARNING,
                "DEADLOCK detected! cycles=%s victim=%s",
                cycles,
                victim,
            )
            self.broadcast(
                _DEADLOCK_DETECTED,
                {"cycles": cycles, "victim": victim},
            )

    def _handle_release(self, msg: Message) -> None:
        tx_id: str | None = msg.payload.get("tx_id")

        if not tx_id:
            self._log(
                logging.WARNING,
                "RELEASE message missing tx_id: %s",
                msg.payload,
            )
            return

        with self._wfg_lock:
            self._wfg.remove_edges_for(tx_id)
            graph_snapshot = self._wfg.get_graph()

        self._log(
            logging.INFO,
            "RELEASE: %s removed | graph=%s",
            tx_id,
            graph_snapshot,
        )

    def report_wait(self, detector_id: int, waiter: str, holder: str) -> None:
        self.send(detector_id, _WAIT_FOR, {"waiter": waiter, "holder": holder})

    def report_release(self, detector_id: int, tx_id: str) -> None:
        self.send(detector_id, _RELEASE, {"tx_id": tx_id})


def run() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.deadlock <node_id> <all_node_ids> <role>")
        print("  role: detector | worker")
        print("Example: python -m src.deadlock 0 0,1,2 detector")
        sys.exit(1)

    node_id = int(sys.argv[1])
    all_node_ids = [int(x) for x in sys.argv[2].split(",")]
    role = sys.argv[3].lower()

    if role not in ("detector", "worker"):
        print(f"Unknown role: {role!r}. Use 'detector' or 'worker'.")
        sys.exit(1)

    node = DeadlockDetector(node_id, all_node_ids)
    node.start()

    detector_id = all_node_ids[0]

    if role == "detector":
        print(
            f"Detector node {node_id} running. "
            "Type 'graph' to display the wait-for graph. "
            "Ctrl-C to quit."
        )
        try:
            while True:
                line = input().strip()
                if line == "graph":
                    print("Wait-for graph:", node._wfg.get_graph())
                elif line:
                    print("Unknown command. Available: graph")
        except (EOFError, KeyboardInterrupt):
            print(f"\nDetector node {node_id} shutting down.")
            sys.exit(0)

    else:
        print(
            f"Worker node {node_id} running. Commands:\n"
            "  wait <waiter> <holder>  — report a wait-for dependency\n"
            "  release <tx_id>         — report transaction completion\n"
            "Ctrl-C to quit."
        )
        try:
            while True:
                line = input().strip()
                if not line:
                    continue
                parts = line.split()
                cmd = parts[0].lower()

                if cmd == "wait" and len(parts) == 3:
                    waiter, holder = parts[1], parts[2]
                    node.report_wait(detector_id, waiter, holder)
                    print(f"Sent WAIT_FOR: {waiter} -> {holder}")
                elif cmd == "release" and len(parts) == 2:
                    tx_id = parts[1]
                    node.report_release(detector_id, tx_id)
                    print(f"Sent RELEASE: {tx_id}")
                else:
                    print(
                        "Unknown command. Use:\n"
                        "  wait <waiter> <holder>\n"
                        "  release <tx_id>"
                    )
        except (EOFError, KeyboardInterrupt):
            print(f"\nWorker node {node_id} shutting down.")
            sys.exit(0)


if __name__ == "__main__":
    run()
