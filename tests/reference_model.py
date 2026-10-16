from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Iterable, Protocol
from uuid import uuid4


class Direction(str, Enum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class Status(str, Enum):
    CREATED = "created"
    IN_PROGRESS = "in_progress"
    PROCESSED = "processed"
    ERROR = "error"
    REJECTED = "rejected"


@dataclass(frozen=True)
class Message:
    exchange: str
    direction: Direction
    message_id: str
    topic: str
    key: str
    body: str
    headers: dict[str, str]
    created_at: datetime
    partition: int = -1
    offset: int = -1
    status: Status = Status.CREATED
    attempts: int = 0
    next_attempt_at: datetime | None = None
    lock_token: str = ""
    locked_until: datetime | None = None
    result_code: str = ""


@dataclass(frozen=True)
class SendResult:
    successful: bool
    acknowledged: bool
    code: str


class ConsumerTransport(Protocol):
    def read(self, limit: int, timeout_ms: int) -> list[Message]: ...

    def commit(self, messages: list[Message]) -> None: ...


class ProducerTransport(Protocol):
    def send(self, message: Message) -> SendResult: ...


class RetryPolicy:
    def __init__(self, max_attempts: int, initial_seconds: int = 30, maximum_seconds: int = 3600):
        if max_attempts < 1 or initial_seconds < 1 or maximum_seconds < initial_seconds:
            raise ValueError("invalid retry policy")
        self.max_attempts = max_attempts
        self.initial_seconds = initial_seconds
        self.maximum_seconds = maximum_seconds

    def delay(self, attempts: int) -> timedelta:
        exponent = max(0, attempts - 1)
        seconds = min(self.maximum_seconds, self.initial_seconds * (2**exponent))
        return timedelta(seconds=seconds)


class InMemoryQueue:
    def __init__(self):
        self._messages: dict[tuple[str, Direction, str], Message] = {}

    @staticmethod
    def _key(message: Message) -> tuple[str, Direction, str]:
        return message.exchange, message.direction, message.message_id

    def add_if_absent(self, message: Message) -> bool:
        key = self._key(message)
        if key in self._messages:
            return False
        self._messages[key] = message
        return True

    def get(self, message: Message) -> Message:
        return self._messages[self._key(message)]

    def claim(self, exchange: str, direction: Direction, now: datetime, limit: int) -> list[Message]:
        candidates = [
            message
            for message in self._messages.values()
            if message.exchange == exchange
            and message.direction is direction
            and message.status in {Status.CREATED, Status.ERROR}
            and (message.next_attempt_at is None or message.next_attempt_at <= now)
            and (message.locked_until is None or message.locked_until <= now)
        ]
        candidates.sort(key=lambda item: (item.created_at, item.message_id))
        claimed: list[Message] = []
        for message in candidates[:limit]:
            updated = replace(
                message,
                status=Status.IN_PROGRESS,
                lock_token=str(uuid4()),
                locked_until=now + timedelta(minutes=5),
            )
            self._messages[self._key(message)] = updated
            claimed.append(updated)
        return claimed

    def complete(self, message: Message, result_code: str = "ok") -> None:
        current = self._locked(message)
        self._messages[self._key(message)] = replace(
            current,
            status=Status.PROCESSED,
            lock_token="",
            locked_until=None,
            next_attempt_at=None,
            result_code=result_code,
        )

    def fail(self, message: Message, code: str, now: datetime, policy: RetryPolicy) -> None:
        current = self._locked(message)
        attempts = current.attempts + 1
        exhausted = attempts >= policy.max_attempts
        self._messages[self._key(message)] = replace(
            current,
            status=Status.REJECTED if exhausted else Status.ERROR,
            attempts=attempts,
            next_attempt_at=None if exhausted else now + policy.delay(attempts),
            lock_token="",
            locked_until=None,
            result_code=code,
        )

    def _locked(self, message: Message) -> Message:
        current = self.get(message)
        if not message.lock_token or current.lock_token != message.lock_token:
            raise RuntimeError("message lock is lost")
        return current


def incoming_message_id(topic: str, partition: int, offset: int) -> str:
    if not topic or partition < 0 or offset < 0:
        raise ValueError("invalid Kafka position")
    return f"{topic}:{partition}:{offset}"


def consume_once(
    queue: InMemoryQueue,
    transport: ConsumerTransport,
    exchange: str,
    limit: int,
    timeout_ms: int,
) -> int:
    messages = transport.read(limit, timeout_ms)
    for message in messages:
        if message.direction is not Direction.INBOUND or message.exchange != exchange:
            raise ValueError("unexpected inbound message")
        queue.add_if_absent(message)
    if messages:
        transport.commit(messages)
    return len(messages)


def produce_once(
    queue: InMemoryQueue,
    transport: ProducerTransport,
    exchange: str,
    now: datetime,
    limit: int,
    policy: RetryPolicy,
) -> int:
    messages = queue.claim(exchange, Direction.OUTBOUND, now, limit)
    for message in messages:
        try:
            result = transport.send(message)
        except Exception:
            queue.fail(message, "transport_exception", now, policy)
            continue
        if result.successful and result.acknowledged:
            queue.complete(message, result.code)
        else:
            queue.fail(message, result.code or "not_acknowledged", now, policy)
    return len(messages)


def process_inbox_once(
    queue: InMemoryQueue,
    handler: Any,
    exchange: str,
    now: datetime,
    limit: int,
    policy: RetryPolicy,
) -> int:
    messages = queue.claim(exchange, Direction.INBOUND, now, limit)
    for message in messages:
        try:
            handler(message)
        except Exception:
            queue.fail(message, "handler_exception", now, policy)
        else:
            queue.complete(message)
    return len(messages)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def messages_at_offsets(exchange: str, topic: str, offsets: Iterable[int]) -> list[Message]:
    now = utc_now()
    return [
        Message(
            exchange=exchange,
            direction=Direction.INBOUND,
            message_id=incoming_message_id(topic, 0, offset),
            topic=topic,
            key="",
            body="{}",
            headers={},
            created_at=now,
            partition=0,
            offset=offset,
        )
        for offset in offsets
    ]
