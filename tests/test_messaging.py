"""Tests for src/messaging.py inter-process communication layer."""

import threading
import time

import pytest

from src.messaging import LamportClock, Message, broadcast, send_message, start_listener


# ---------------------------------------------------------------------------
# LamportClock
# ---------------------------------------------------------------------------


class TestLamportClock:
    def test_increment_returns_one_on_first_call(self):
        clock = LamportClock()
        assert clock.increment() == 1

    def test_increment_monotonically_increasing(self):
        clock = LamportClock()
        values = [clock.increment() for _ in range(5)]
        assert values == sorted(values)
        assert values == list(range(1, 6))

    def test_update_max_plus_one_when_received_is_greater(self):
        clock = LamportClock()
        clock.increment()  # local = 1
        result = clock.update(10)
        assert result == 11  # max(1, 10) + 1

    def test_update_max_plus_one_when_local_is_greater(self):
        clock = LamportClock()
        for _ in range(5):
            clock.increment()  # local = 5
        result = clock.update(3)
        assert result == 6  # max(5, 3) + 1

    def test_update_max_plus_one_when_values_are_equal(self):
        clock = LamportClock()
        for _ in range(4):
            clock.increment()  # local = 4
        result = clock.update(4)
        assert result == 5  # max(4, 4) + 1

    def test_update_with_zero_received(self):
        clock = LamportClock()
        clock.increment()  # local = 1
        result = clock.update(0)
        assert result == 2  # max(1, 0) + 1

    def test_value_property_does_not_modify_clock(self):
        clock = LamportClock()
        clock.increment()
        v1 = clock.value
        v2 = clock.value
        assert v1 == v2 == 1

    def test_update_advances_clock_beyond_increment(self):
        clock = LamportClock()
        clock.increment()  # local = 1
        clock.update(100)  # local = 101
        # Next increment should continue from 101
        result = clock.increment()
        assert result == 102


# ---------------------------------------------------------------------------
# Message serialization
# ---------------------------------------------------------------------------


class TestMessageSerialization:
    def test_roundtrip_preserves_all_fields(self):
        msg = Message(
            type="TEST",
            sender_id=42,
            payload={"key": "value", "num": 99},
            lamport_clock=7,
        )
        restored = Message.from_json(msg.to_json())
        assert restored.type == msg.type
        assert restored.sender_id == msg.sender_id
        assert restored.payload == msg.payload
        assert restored.lamport_clock == msg.lamport_clock

    def test_to_json_returns_string(self):
        msg = Message(type="PING", sender_id=0, payload={}, lamport_clock=1)
        assert isinstance(msg.to_json(), str)

    def test_empty_payload_roundtrip(self):
        msg = Message(type="ACK", sender_id=1, payload={}, lamport_clock=0)
        restored = Message.from_json(msg.to_json())
        assert restored.payload == {}

    def test_nested_payload_roundtrip(self):
        payload = {"nested": {"a": 1, "b": [1, 2, 3]}}
        msg = Message(type="DATA", sender_id=5, payload=payload, lamport_clock=3)
        restored = Message.from_json(msg.to_json())
        assert restored.payload == payload

    def test_from_json_invalid_raises(self):
        with pytest.raises((KeyError, Exception)):
            Message.from_json('{"bad": "data"}')


# ---------------------------------------------------------------------------
# send_message + start_listener roundtrip
#
# Uses node_id=10 → port 5010.
# ---------------------------------------------------------------------------


def test_send_receive_roundtrip():
    node_id = 10
    received_messages = []
    event = threading.Event()

    def callback(msg: Message):
        received_messages.append(msg)
        event.set()

    start_listener(node_id, callback)
    time.sleep(0.05)  # allow listener thread to bind

    msg = Message(
        type="HELLO",
        sender_id=99,
        payload={"data": "world"},
        lamport_clock=3,
    )
    send_message(node_id, msg)

    assert event.wait(timeout=2.0), "Timed out: listener did not receive the message"
    assert len(received_messages) == 1
    recv = received_messages[0]
    assert recv.type == "HELLO"
    assert recv.sender_id == 99
    assert recv.payload == {"data": "world"}
    assert recv.lamport_clock == 3


# ---------------------------------------------------------------------------
# broadcast delivery
#
# Listeners on node_ids 11, 12 → ports 5011, 5012.
# Sender is node_id=10 (no listener — broadcast excludes self).
# ---------------------------------------------------------------------------


def _make_callback(event: threading.Event, messages: list):
    """Factory to produce a per-node message callback that avoids closure bugs."""

    def callback(msg: Message):
        messages.append(msg)
        event.set()

    return callback


def test_broadcast_delivery():
    sender_id = 10
    receiver_ids = [11, 12]
    all_node_ids = [sender_id] + receiver_ids

    events: dict[int, threading.Event] = {}
    received: dict[int, list] = {}

    for nid in receiver_ids:
        events[nid] = threading.Event()
        received[nid] = []
        start_listener(nid, _make_callback(events[nid], received[nid]))

    time.sleep(0.05)  # allow all listeners to bind

    broadcast(
        sender_id=sender_id,
        msg_type="BCAST",
        payload={"msg": "hello all"},
        clock=5,
        node_ids=all_node_ids,
    )

    for nid in receiver_ids:
        assert events[nid].wait(timeout=2.0), (
            f"Node {nid} did not receive the broadcast"
        )
        assert len(received[nid]) == 1
        msg = received[nid][0]
        assert msg.type == "BCAST"
        assert msg.sender_id == sender_id
        assert msg.payload == {"msg": "hello all"}
        assert msg.lamport_clock == 5


def test_broadcast_does_not_send_to_self():
    """Broadcast skips the sender; broadcasting to a single-node list delivers nothing."""
    # broadcast() filters out sender_id from node_ids, so if node_ids contains
    # only the sender, no send_message call is made and no OSError is raised.
    sender_clock = LamportClock()
    clock_val = sender_clock.increment()

    # Should complete without error even though no listener is running for node_id=10.
    broadcast(
        sender_id=10,
        msg_type="SELF_TEST",
        payload={},
        clock=clock_val,
        node_ids=[10],  # only self — nothing should be sent
    )


# ---------------------------------------------------------------------------
# Lamport clock ordering
#
# After a send/receive cycle, the receiver's clock must be >= sender's clock
# at send time. Uses node_id=13 → port 5013.
# ---------------------------------------------------------------------------


def test_lamport_clock_ordering():
    node_id = 13
    sender_clock = LamportClock()
    receiver_clock = LamportClock()

    received_event = threading.Event()
    receiver_clock_values: list[int] = []

    def callback(msg: Message):
        new_val = receiver_clock.update(msg.lamport_clock)
        receiver_clock_values.append(new_val)
        received_event.set()

    start_listener(node_id, callback)
    time.sleep(0.05)

    send_clock_value = sender_clock.increment()
    msg = Message(
        type="ORDER_TEST",
        sender_id=99,
        payload={},
        lamport_clock=send_clock_value,
    )
    send_message(node_id, msg)

    assert received_event.wait(timeout=2.0), (
        "Timed out: clock ordering message not received"
    )
    assert len(receiver_clock_values) == 1
    # Receiver's updated clock must be strictly greater than the sender's clock
    # (max(local, received) + 1 guarantees receiver_clock > send_clock_value).
    assert receiver_clock_values[0] > send_clock_value


def test_lamport_clock_ordering_with_high_sender_clock():
    """Receiver clock must catch up when sender's clock is far ahead."""
    sender_clock = LamportClock()
    receiver_clock = LamportClock()

    # Simulate receiver being behind
    receiver_clock.increment()  # receiver local = 1

    # Sender is far ahead
    for _ in range(20):
        sender_clock.increment()  # sender local = 20

    send_clock_value = sender_clock.value
    updated = receiver_clock.update(send_clock_value)

    assert updated == send_clock_value + 1
    assert updated > send_clock_value
