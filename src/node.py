import logging
import os
import socket
import sys
import threading

from src.messaging import LamportClock, Message, broadcast, send_message, start_listener

_LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def _build_logger(node_id: int) -> logging.Logger:
    logger = logging.getLogger(f"node.{node_id}")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(f"[node {node_id}][%(clock)s] %(levelname)s %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(_LOGS_DIR, exist_ok=True)
    log_path = os.path.join(_LOGS_DIR, f"node_{node_id}.log")
    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


class _ClockFilter(logging.Filter):
    def __init__(self, clock: LamportClock) -> None:
        super().__init__()
        self._clock = clock

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.clock = self._clock.value
        return True


class Node:
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        self.node_id = node_id
        self.all_node_ids = list(all_node_ids)
        self._clock = LamportClock()
        self._logger = _build_logger(node_id)
        self._logger.addFilter(_ClockFilter(self._clock))
        self._listener_thread: threading.Thread | None = None
        self._server_sock: socket.socket | None = None

    def start(self) -> None:
        self._listener_thread, self._server_sock = start_listener(
            self.node_id, self.handle_message
        )
        self._log(logging.INFO, "Listener started on port %d.", 5000 + self.node_id)

    def stop(self) -> None:
        if self._server_sock is not None:
            try:
                self._server_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

    def send(self, target_id: int, msg_type: str, payload: dict) -> None:
        clock = self._clock.increment()
        msg = Message(
            type=msg_type,
            sender_id=self.node_id,
            payload=payload,
            lamport_clock=clock,
        )
        send_message(target_id, msg)
        self._log(
            logging.DEBUG,
            "SEND -> node %d | type=%s clock=%d",
            target_id,
            msg_type,
            clock,
        )

    def broadcast(self, msg_type: str, payload: dict) -> None:
        clock = self._clock.increment()
        broadcast(self.node_id, msg_type, payload, clock, self.all_node_ids)
        self._log(
            logging.DEBUG,
            "BROADCAST | type=%s clock=%d peers=%s",
            msg_type,
            clock,
            [n for n in self.all_node_ids if n != self.node_id],
        )

    def handle_message(self, msg: Message) -> None:
        new_clock = self._clock.update(msg.lamport_clock)
        self._log(
            logging.INFO,
            "RECV from node %d | type=%s sender_clock=%d local_clock=%d payload=%s",
            msg.sender_id,
            msg.type,
            msg.lamport_clock,
            new_clock,
            msg.payload,
        )

    def _log(self, level: int, msg: str, *args) -> None:
        self._logger.log(level, msg, *args)

    @classmethod
    def run(cls) -> None:
        if len(sys.argv) < 3:
            print("Usage: python -m src.node <node_id> <all_node_ids>")
            print("Example: python -m src.node 0 0,1,2")
            sys.exit(1)

        node_id = int(sys.argv[1])
        all_node_ids = [int(x) for x in sys.argv[2].split(",")]

        node = cls(node_id, all_node_ids)
        node.start()

        print(
            f"Node {node_id} running. Type a message and press Enter to broadcast. Ctrl-C to quit."
        )

        try:
            while True:
                line = input()
                if line:
                    node.broadcast("BROADCAST", {"text": line})
        except (EOFError, KeyboardInterrupt):
            print(f"\nNode {node_id} shutting down.")
            sys.exit(0)


if __name__ == "__main__":
    Node.run()
