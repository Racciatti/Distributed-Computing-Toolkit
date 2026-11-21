import logging
import sys
import threading
import time

from src.messaging import Message, broadcast as _low_level_broadcast
from src.node import Node


class MutexNode(Node):
    def __init__(self, node_id: int, all_node_ids: list[int]) -> None:
        super().__init__(node_id, all_node_ids)

        # Algorithm state — all guarded by _mutex_lock
        self._requesting: bool = False
        self._in_cs: bool = False
        self._request_clock: int = 0
        self._reply_count: int = 0
        self._deferred: list[int] = []

        self._mutex_lock = threading.Lock()
        self._cs_event = threading.Event()

    def acquire(self) -> None:
        with self._mutex_lock:
            self._requesting = True
            self._in_cs = False
            self._reply_count = 0
            self._cs_event.clear()
            clock = self._clock.increment()
            self._request_clock = clock

        peers = [n for n in self.all_node_ids if n != self.node_id]
        self._log(
            logging.INFO,
            "MUTEX acquire — broadcasting REQUEST clock=%d peers=%s",
            clock,
            peers,
        )

        # Single-node cluster: no peers to wait for — enter the CS immediately.
        if len(peers) == 0:
            with self._mutex_lock:
                self._in_cs = True
                self._requesting = False
            self._log(
                logging.INFO, "MUTEX acquire — entered critical section (no peers)"
            )
            return

        # Broadcast outside the lock to avoid blocking incoming message handlers.
        _low_level_broadcast(
            self.node_id,
            "MUTEX_REQUEST",
            {"request_clock": clock},
            clock,
            self.all_node_ids,
        )

        # Wait until all N-1 peers have replied.
        granted = self._cs_event.wait(timeout=10.0)
        if not granted:
            raise TimeoutError("Ricart-Agrawala acquire timed out waiting for replies")

        with self._mutex_lock:
            self._in_cs = True

        self._log(logging.INFO, "MUTEX acquire — entered critical section")

    def release(self) -> None:
        with self._mutex_lock:
            self._in_cs = False
            self._requesting = False
            deferred = list(self._deferred)
            self._deferred.clear()

        self._log(
            logging.INFO,
            "MUTEX release — sending deferred replies to %s",
            deferred,
        )

        for target_id in deferred:
            try:
                self.send(target_id, "MUTEX_REPLY", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MUTEX release — could not reach deferred node %d",
                    target_id,
                )

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type == "MUTEX_REQUEST":
            self._handle_request(msg)
        elif msg.type == "MUTEX_REPLY":
            self._handle_reply(msg)

    def _handle_request(self, msg: Message) -> None:
        their_clock = msg.payload["request_clock"]
        their_id = msg.sender_id

        with self._mutex_lock:
            our_clock = self._request_clock
            # Always defer when we currently hold the CS.
            # When we are in the "wanting" state (requesting but not yet in CS),
            # defer only if we have higher priority (lower timestamp/id).
            should_reply = not self._requesting or (
                not self._in_cs and (their_clock, their_id) < (our_clock, self.node_id)
            )
            if not should_reply:
                self._deferred.append(their_id)

        self._log(
            logging.DEBUG,
            "MUTEX _handle_request from node %d (clock=%d) — %s",
            their_id,
            their_clock,
            "REPLY now" if should_reply else "deferred",
        )

        if should_reply:
            try:
                self.send(their_id, "MUTEX_REPLY", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MUTEX _handle_request — could not send REPLY to node %d",
                    their_id,
                )

    def _handle_reply(self, msg: Message) -> None:
        peers = [n for n in self.all_node_ids if n != self.node_id]

        with self._mutex_lock:
            self._reply_count += 1
            count = self._reply_count

        self._log(
            logging.DEBUG,
            "MUTEX _handle_reply from node %d — count=%d/%d",
            msg.sender_id,
            count,
            len(peers),
        )

        if count >= len(peers):
            self._cs_event.set()


class MaekawaMutexNode(Node):
    def __init__(
        self,
        node_id: int,
        all_node_ids: list[int],
        quorum: list[int],
    ) -> None:
        super().__init__(node_id, all_node_ids)

        self._quorum: list[int] = list(quorum)

        # Requesting / CS state
        self._requesting: bool = False
        self._in_cs: bool = False
        self._request_clock: int = 0

        # GRANT collection for the current acquire round
        self._grant_count: int = 0

        # Quorum-member state: which node this node has currently granted
        self._voted_for: int | None = None
        # Lamport clock of the request we granted (for priority comparison)
        self._voted_for_clock: int | None = None
        # True when we have sent MK_INQUIRE and are awaiting RELINQUISH/RELEASE
        self._inquired: bool = False

        # Deferred requests: list of (lamport_clock, sender_node_id)
        self._pending_queue: list[tuple[int, int]] = []

        self._maekawa_lock = threading.Lock()
        self._grant_event = threading.Event()

    def acquire(self) -> None:
        with self._maekawa_lock:
            self._requesting = True
            self._in_cs = False
            self._grant_event.clear()
            clock = self._clock.increment()
            self._request_clock = clock

            # Self-grant: if this node is in its own quorum, count the vote
            # immediately without sending a network message.
            if self.node_id in self._quorum:
                self._grant_count = 1
                self._voted_for = self.node_id
                self._voted_for_clock = clock
            else:
                self._grant_count = 0

        quorum_peers = [n for n in self._quorum if n != self.node_id]

        self._log(
            logging.INFO,
            "MAEKAWA acquire — sending MK_REQUEST clock=%d quorum_peers=%s",
            clock,
            quorum_peers,
        )

        for peer in quorum_peers:
            try:
                self.send(peer, "MK_REQUEST", {"request_clock": clock})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA acquire — could not reach quorum member %d",
                    peer,
                )

        # If the quorum consists only of self (or is empty), the grant count
        # already satisfies the threshold — set the event now so wait() returns.
        with self._maekawa_lock:
            if self._grant_count >= len(self._quorum):
                self._grant_event.set()

        granted = self._grant_event.wait(timeout=10.0)
        if not granted:
            raise TimeoutError("Maekawa acquire timed out waiting for quorum grants")

        with self._maekawa_lock:
            self._in_cs = True

        self._log(logging.INFO, "MAEKAWA acquire — entered critical section")

    def release(self) -> None:
        with self._maekawa_lock:
            self._in_cs = False
            self._requesting = False

        quorum_peers = [n for n in self._quorum if n != self.node_id]

        self._log(
            logging.INFO,
            "MAEKAWA release — sending MK_RELEASE to quorum_peers=%s",
            quorum_peers,
        )

        for peer in quorum_peers:
            try:
                self.send(peer, "MK_RELEASE", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA release — could not reach quorum member %d",
                    peer,
                )

        # Self-release: if this node is in its own quorum it must process the
        # implied release locally (clear vote, grant next waiter if any).
        if self.node_id in self._quorum:
            self._process_release()

    def handle_message(self, msg: Message) -> None:
        super().handle_message(msg)

        if msg.type == "MK_REQUEST":
            self._handle_request(msg)
        elif msg.type == "MK_GRANT":
            self._handle_grant(msg)
        elif msg.type == "MK_RELEASE":
            self._handle_release(msg)
        elif msg.type == "MK_INQUIRE":
            self._handle_inquire(msg)
        elif msg.type == "MK_RELINQUISH":
            self._handle_relinquish(msg)

    def _handle_request(self, msg: Message) -> None:
        req_clock = msg.payload["request_clock"]
        req_node = msg.sender_id
        grant = False
        do_inquire = False
        inquire_target: int | None = None

        with self._maekawa_lock:
            if self._voted_for is None:
                self._voted_for = req_node
                self._voted_for_clock = req_clock
                grant = True
            else:
                self._pending_queue.append((req_clock, req_node))
                # Send INQUIRE if the incoming request has higher priority than
                # the node we already granted, we are not currently in the CS
                # ourselves, and we have not already sent an unanswered INQUIRE.
                if (
                    not self._in_cs
                    and not self._inquired
                    and self._voted_for_clock is not None
                    and (req_clock, req_node) < (self._voted_for_clock, self._voted_for)
                ):
                    inquire_target = self._voted_for
                    self._inquired = True
                    do_inquire = True

        self._log(
            logging.DEBUG,
            "MAEKAWA _handle_request from node %d (clock=%d) — %s",
            req_node,
            req_clock,
            "GRANT now" if grant else f"queued (queue size={len(self._pending_queue)})",
        )

        if grant:
            try:
                self.send(req_node, "MK_GRANT", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA _handle_request — could not send MK_GRANT to node %d",
                    req_node,
                )
        elif do_inquire and inquire_target is not None:
            self._log(
                logging.DEBUG,
                "MAEKAWA _handle_request — sending MK_INQUIRE to node %d"
                " (higher-priority request from node %d clock=%d)",
                inquire_target,
                req_node,
                req_clock,
            )
            try:
                self.send(inquire_target, "MK_INQUIRE", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA _handle_request — could not send MK_INQUIRE to node %d",
                    inquire_target,
                )

    def _handle_grant(self, msg: Message) -> None:
        with self._maekawa_lock:
            self._grant_count += 1
            done = self._grant_count >= len(self._quorum)

        self._log(
            logging.DEBUG,
            "MAEKAWA _handle_grant from node %d — count=%d/%d",
            msg.sender_id,
            self._grant_count,
            len(self._quorum),
        )

        if done:
            self._grant_event.set()

    def _handle_release(self, msg: Message) -> None:
        self._log(
            logging.DEBUG,
            "MAEKAWA _handle_release from node %d",
            msg.sender_id,
        )
        self._process_release()

    def _process_release(self) -> None:
        next_request: tuple[int, int] | None = None

        with self._maekawa_lock:
            self._voted_for = None
            self._voted_for_clock = None
            self._inquired = False
            if self._pending_queue:
                # Highest priority = lowest (clock, node_id) tuple.
                self._pending_queue.sort()
                next_request = self._pending_queue.pop(0)
                self._voted_for = next_request[1]
                self._voted_for_clock = next_request[0]

        if next_request is not None:
            req_clock, req_node = next_request
            self._log(
                logging.DEBUG,
                "MAEKAWA _process_release — granting to node %d (clock=%d)",
                req_node,
                req_clock,
            )
            try:
                self.send(req_node, "MK_GRANT", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA _process_release — could not send MK_GRANT to node %d",
                    req_node,
                )

    def _handle_inquire(self, msg: Message) -> None:
        relinquish = False

        with self._maekawa_lock:
            if self._requesting and not self._in_cs:
                # Give back the grant from this quorum member.
                self._grant_count -= 1
                relinquish = True

        self._log(
            logging.DEBUG,
            "MAEKAWA _handle_inquire from node %d — %s",
            msg.sender_id,
            "sending RELINQUISH" if relinquish else "ignored (in CS or not requesting)",
        )

        if relinquish:
            try:
                self.send(msg.sender_id, "MK_RELINQUISH", {})
            except OSError:
                self._log(
                    logging.WARNING,
                    "MAEKAWA _handle_inquire — could not send MK_RELINQUISH to node %d",
                    msg.sender_id,
                )

    def _handle_relinquish(self, msg: Message) -> None:
        self._log(
            logging.DEBUG,
            "MAEKAWA _handle_relinquish from node %d",
            msg.sender_id,
        )

        with self._maekawa_lock:
            # Re-enqueue the original grantee so it gets served again later.
            if self._voted_for is not None and self._voted_for_clock is not None:
                self._pending_queue.append((self._voted_for_clock, self._voted_for))

        self._process_release()


def run() -> None:
    if len(sys.argv) < 3:
        print("Usage: python -m src.mutex <node_id> <all_node_ids>")
        print("Example: python -m src.mutex 0 0,1,2")
        sys.exit(1)

    node_id = int(sys.argv[1])
    all_node_ids = [int(x) for x in sys.argv[2].split(",")]

    node = MutexNode(node_id, all_node_ids)
    node.start()

    print(
        f"Node {node_id} running. "
        "Type 'acquire' and press Enter to request the critical section. "
        "Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line.strip() == "acquire":
                node.acquire()
                print("ENTERED CS")
                time.sleep(2)
                node.release()
                print("RELEASED CS")
    except (EOFError, KeyboardInterrupt):
        print(f"\nNode {node_id} shutting down.")
        sys.exit(0)


def run_maekawa() -> None:
    if len(sys.argv) < 5:
        print(
            "Usage: python -m src.mutex maekawa <node_id> <all_node_ids> <quorum_ids>"
        )
        print("Example: python -m src.mutex maekawa 0 0,1,2 0,1")
        sys.exit(1)

    node_id = int(sys.argv[2])
    all_node_ids = [int(x) for x in sys.argv[3].split(",")]
    quorum = [int(x) for x in sys.argv[4].split(",")]

    node = MaekawaMutexNode(node_id, all_node_ids, quorum)
    node.start()

    print(
        f"Node {node_id} running (Maekawa). "
        "Type 'acquire' and press Enter to request the critical section. "
        f"Quorum: {quorum}. Ctrl-C to quit."
    )

    try:
        while True:
            line = input()
            if line.strip() == "acquire":
                node.acquire()
                print("ENTERED CS")
                time.sleep(2)
                node.release()
                print("RELEASED CS")
    except (EOFError, KeyboardInterrupt):
        print(f"\nNode {node_id} shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "maekawa":
        run_maekawa()
    else:
        run()
