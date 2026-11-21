import json
import socket
import threading
from dataclasses import asdict, dataclass


HOST = "127.0.0.1"
BASE_PORT = 5000


def port_for(node_id: int) -> int:
    return BASE_PORT + node_id


class LamportClock:
    def __init__(self) -> None:
        self._time: int = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received: int) -> int:
        with self._lock:
            self._time = max(self._time, received) + 1
            return self._time

    @property
    def value(self) -> int:
        with self._lock:
            return self._time


@dataclass
class Message:
    type: str
    sender_id: int
    payload: dict
    lamport_clock: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Message":
        obj = json.loads(data)
        return cls(
            type=obj["type"],
            sender_id=obj["sender_id"],
            payload=obj["payload"],
            lamport_clock=obj["lamport_clock"],
        )


def send_message(target_id: int, msg: Message) -> None:
    host = HOST
    port = port_for(target_id)
    raw = msg.to_json().encode("utf-8")
    length_prefix = len(raw).to_bytes(4, byteorder="big")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.sendall(length_prefix + raw)


def broadcast(
    sender_id: int,
    msg_type: str,
    payload: dict,
    clock: int,
    node_ids: list[int],
) -> None:
    msg = Message(
        type=msg_type, sender_id=sender_id, payload=payload, lamport_clock=clock
    )
    for nid in node_ids:
        if nid == sender_id:
            continue
        try:
            send_message(nid, msg)
        except OSError:
            pass


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed before all bytes received.")
        buf += chunk
    return buf


def _handle_connection(conn: socket.socket, on_message_callback) -> None:
    try:
        with conn:
            length_bytes = _recv_exact(conn, 4)
            length = int.from_bytes(length_bytes, byteorder="big")
            raw = _recv_exact(conn, length)
            msg = Message.from_json(raw.decode("utf-8"))
            on_message_callback(msg)
    except (OSError, ConnectionError, json.JSONDecodeError, KeyError):
        pass


def start_listener(
    node_id: int, on_message_callback
) -> tuple[threading.Thread, socket.socket]:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, port_for(node_id)))
    srv.listen()

    def _serve() -> None:
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            t = threading.Thread(
                target=_handle_connection,
                args=(conn, on_message_callback),
                daemon=True,
            )
            t.start()

    thread = threading.Thread(target=_serve, daemon=True, name=f"listener-{node_id}")
    thread.start()
    return thread, srv
