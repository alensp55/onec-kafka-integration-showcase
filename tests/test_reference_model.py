from __future__ import annotations

import unittest
from datetime import timedelta

from reference_model import (
    Direction,
    InMemoryQueue,
    Message,
    RetryPolicy,
    SendResult,
    Status,
    consume_once,
    incoming_message_id,
    messages_at_offsets,
    process_inbox_once,
    produce_once,
    utc_now,
)


class ConsumerStub:
    def __init__(self, messages):
        self.messages = messages
        self.committed = []

    def read(self, limit, timeout_ms):
        return self.messages[:limit]

    def commit(self, messages):
        self.committed.append(list(messages))


class ProducerStub:
    def __init__(self, results):
        self.results = iter(results)
        self.sent = []

    def send(self, message):
        self.sent.append(message.message_id)
        result = next(self.results)
        if isinstance(result, Exception):
            raise result
        return result


def outbound(exchange="main", message_id="event-1"):
    return Message(
        exchange=exchange,
        direction=Direction.OUTBOUND,
        message_id=message_id,
        topic="events",
        key=message_id,
        body="{}",
        headers={},
        created_at=utc_now(),
    )


class KafkaIntegrationTests(unittest.TestCase):
    def test_incoming_id_is_stable_for_kafka_position(self):
        self.assertEqual("events:3:42", incoming_message_id("events", 3, 42))

    def test_consumer_commits_only_after_messages_are_saved(self):
        queue = InMemoryQueue()
        messages = messages_at_offsets("main", "events", [1, 2])
        transport = ConsumerStub(messages)

        self.assertEqual(2, consume_once(queue, transport, "main", 10, 1000))
        self.assertEqual([messages], transport.committed)
        self.assertEqual(Status.CREATED, queue.get(messages[0]).status)

    def test_consumer_is_idempotent(self):
        queue = InMemoryQueue()
        message = messages_at_offsets("main", "events", [1])[0]
        transport = ConsumerStub([message])

        consume_once(queue, transport, "main", 10, 1000)
        consume_once(queue, transport, "main", 10, 1000)

        self.assertFalse(queue.add_if_absent(message))
        self.assertEqual(2, len(transport.committed))

    def test_producer_completes_only_acknowledged_message(self):
        queue = InMemoryQueue()
        message = outbound()
        queue.add_if_absent(message)
        transport = ProducerStub([SendResult(True, True, "ack")])

        produce_once(queue, transport, "main", utc_now(), 10, RetryPolicy(3))

        self.assertEqual(Status.PROCESSED, queue.get(message).status)

    def test_producer_schedules_retry_without_acknowledgement(self):
        queue = InMemoryQueue()
        message = outbound()
        queue.add_if_absent(message)
        now = utc_now()
        transport = ProducerStub([SendResult(True, False, "no_ack")])

        produce_once(queue, transport, "main", now, 10, RetryPolicy(3, 10, 60))

        stored = queue.get(message)
        self.assertEqual(Status.ERROR, stored.status)
        self.assertEqual(now + timedelta(seconds=10), stored.next_attempt_at)

    def test_retry_policy_is_bounded(self):
        policy = RetryPolicy(5, 10, 25)
        self.assertEqual(timedelta(seconds=10), policy.delay(1))
        self.assertEqual(timedelta(seconds=20), policy.delay(2))
        self.assertEqual(timedelta(seconds=25), policy.delay(3))
        self.assertEqual(timedelta(seconds=25), policy.delay(10))

    def test_exhausted_message_is_rejected(self):
        queue = InMemoryQueue()
        message = outbound()
        queue.add_if_absent(message)
        policy = RetryPolicy(2)
        first = ProducerStub([RuntimeError("temporary")])
        now = utc_now()

        produce_once(queue, first, "main", now, 10, policy)
        second = ProducerStub([RuntimeError("temporary")])
        produce_once(queue, second, "main", now + timedelta(minutes=1), 10, policy)

        self.assertEqual(Status.REJECTED, queue.get(message).status)

    def test_inbox_handler_has_independent_retry(self):
        queue = InMemoryQueue()
        message = messages_at_offsets("main", "events", [1])[0]
        queue.add_if_absent(message)

        def fail(_message):
            raise ValueError("invalid payload")

        process_inbox_once(queue, fail, "main", utc_now(), 10, RetryPolicy(3))

        self.assertEqual(Status.ERROR, queue.get(message).status)


if __name__ == "__main__":
    unittest.main()
