import atexit
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException
from django.db import transaction

from app.settings import KAFKA_PORT
from chat.models import ConversationMessage

logger = logging.getLogger(__name__)


class KafkaBatchConsumer:
    def __init__(
            self,
            topic: str = "chat-messages",
            group_id: str = "chat-consumer-group",
            bootstrap_servers: str = f"localhost:{KAFKA_PORT}",
            batch_size: int = 100,
            max_interval_seconds: float = 60.0,
            poll_timeout: float = 1.0,
            consumer_config: Optional[Dict[str, Any]] = None,
        ):
            self.topic = topic
            self.group_id = group_id
            self.bootstrap = bootstrap_servers
            self.batch_size = max(1, int(batch_size))
            self.max_interval_seconds = float(max_interval_seconds)
            self.poll_timeout = float(poll_timeout)

            self._buffer: List[Tuple[Dict[str, Any], Any]] = []
            self._lock = threading.Lock()
            self._stop_event = threading.Event()
            self._last_flush_time = time.time()

            conf = {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
            if consumer_config:
                conf.update(consumer_config)

            self.consumer = Consumer(conf)
            self.consumer.subscribe([self.topic])
            logger.info("KafkaBatchConsumer subscribed to topic=%s group=%s", self.topic, self.group_id)

    def run(self):
        try:
            while not self._stop_event.is_set():
                msg = self.consumer.poll(timeout=self.poll_timeout)
                now = time.time()

                if msg is None:
                    if self._should_time_flush(now):
                        self._flush_if_needed()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition: %s", msg.error())
                        continue
                    logger.error("Kafka consumer error: %s", msg.error())
                    time.sleep(0.5)
                    continue

                try:
                    raw = msg.value()
                    if raw is None:
                        logger.warning("Skipping the message with empty payload : %s", msg)
                        continue
                    if isinstance(raw, bytes):
                        payload = json.loads(raw.decode("utf-8"))
                    elif isinstance(raw, str):
                        payload = json.loads(raw)
                    else:
                        payload = json.loads(str(raw))
                except Exception as exc:
                    logger.exception("Failed to decode message payload: %s -- skipping. raw=%r", exc, msg.value())
                    try:
                        self.consumer.commit(message=msg)
                    except Exception:
                        logger.exception("Failed to comit offset for malformed message")
                    continue

                with self._lock:
                    self._buffer.append((payload, msg))
                    buffer_len = len(self._buffer)
                logger.debug("Buffered message; buffer_len=%d", buffer_len)

                if buffer_len >= self.batch_size:
                    logger.debug("Triggering flush because buffer_len >= batch_size (%d >= %d)", buffer_len, self.batch_size)
                    self._flush_if_needed()

                if self._should_time_flush(now):
                    self._flush_if_needed(force=True)

        except KeyboardInterrupt:
            logger.info("KafkaBatchConsumer received KeyboardInterrupt, shutting down.")
        except Exception:
            logger.exception("Unexpected error in KafkaBatchConsumer main loop.")
        finally:
            # flush remaining messages before exit
            logger.info("KafkaBatchConsumer flushing remaining messages before close.")
            self._flush_if_needed(force=True)
            try:
                self.consumer.close()
            except Exception:
                logger.exception("Error closing Kafka consumer")
            logger.info("KafkaBatchConsumer stopped.")

    def stop(self):
        self._stop_event.set()

    def _should_time_flush(self, now: Optional[float] = None) -> bool:
        if now is None:
            now = time.time()
        with self._lock:
            if not self._buffer:
                return False
        return (now - self._last_flush_time) >= self.max_interval_seconds

    def _flush_if_needed(self, force: bool = False) -> None:
        with self._lock:
            if not self._buffer:
                logger.debug("Flush skipped: buffer empty")
                return
            if not force and len(self._buffer) < self.batch_size:
                logger.debug("Flush skipped: buffer_len (%d) < batch_size (%d) and not forced", len(self._buffer), self.batch_size)
                return
            to_flush = self._buffer
            self._buffer = []
            self._last_flush_time = time.time()

        logger.debug("Flushing %d messages (force=%s)", len(to_flush), force)

        try:
            self._persist_batch(to_flush)
        except Exception:
            logger.exception("Failed to persist batch. Re-queueing messages into buffer for retry.")
            with self._lock:
                self._buffer = to_flush + self._buffer
            time.sleep(1)

    def _persist_batch(self, batch: List[Tuple[Dict[str, Any], Any]]) -> None:
        objs = []
        kafka_messages_to_commit = []

        for payload, km in batch:
            try:
                conversation_id = payload.get("conversation_id")
                sender_id = payload.get("sender_id")
                content = payload.get("content", "") or ""
                message_type = payload.get("message_type", "TEXT") or "TEXT"
                reply_to_id = payload.get("reply_to_id") or None

                if conversation_id is None or sender_id is None:
                    logger.warning("Skipping message without conversation_id or sender_id: %r", payload)
                    continue

                cm = ConversationMessage(
                    conversation_id=int(conversation_id),
                    sender_id=int(sender_id),
                    content=content,
                    message_type=message_type,
                    reply_to_id=(int(reply_to_id) if reply_to_id is not None else None),
                )
                objs.append(cm)
                kafka_messages_to_commit.append(km)
            except Exception:
                logger.exception("Malformed payload; skipping. payload = %r", payload)
                continue

        if not objs:
            for _, km in batch:
                try:
                    self.consumer.commit(message=km)
                except Exception:
                    logger.exception("Failed to commit offset for skipped message")
            return

        try:
            with transaction.atomic():
                ConversationMessage.objects.bulk_create(objs, ignore_conflicts=True)
        except Exception as exc:
            logger.exception("DB bulk_create failed: %s", exc)
            raise

        for km in kafka_messages_to_commit:
            try:
                self.consumer.commit(message=km)
            except Exception:
                logger.exception("Failed to commit offset for message; msg=%r", km)

        logger.info("Persisted and committed %d messages", len(objs))
        _start_post_persist_countdown()


_background_consumer: Optional[KafkaBatchConsumer] = None
_background_thread: Optional[threading.Thread] = None
_background_lock = threading.Lock()


def _countdown_worker(seconds: int = 60) -> None:
    for remaining in range(seconds, -1, -1):
        logger.debug("messages sent to db; countdown=%d", remaining)
        time.sleep(1.0)


def _start_post_persist_countdown(seconds: int = 60) -> None:
    threading.Thread(target=_countdown_worker, args=(seconds,), daemon=True).start()


def _consumer_thread_target(consumer: KafkaBatchConsumer) -> None:
    try:
        consumer.run()
    except Exception:
        logger.exception("Background Kafka consumer crashed")
    finally:
        global _background_consumer, _background_thread
        with _background_lock:
            _background_consumer = None
            _background_thread = None


def start_background_consumer(
    *,
    topic: str = "chat-messages",
    group_id: str = "chat-consumer-group",
    bootstrap_servers: str = f"localhost:{KAFKA_PORT}",
    batch_size: int = 100,
    max_interval_seconds: float = 60.0,
    poll_timeout: float = 1.0,
    consumer_config: Optional[Dict[str, Any]] = None,
) -> KafkaBatchConsumer:
    """Start a singleton background Kafka consumer thread if not already running."""
    global _background_consumer, _background_thread

    with _background_lock:
        if _background_thread and _background_thread.is_alive() and _background_consumer:
            return _background_consumer

        consumer = KafkaBatchConsumer(
            topic=topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            batch_size=batch_size,
            max_interval_seconds=max_interval_seconds,
            poll_timeout=poll_timeout,
            consumer_config=consumer_config,
        )
        thread = threading.Thread(target=_consumer_thread_target, args=(consumer,), daemon=True)
        thread.start()

        logger.info("Background Kafka consumer thread started in PID %s", os.getpid())

        _background_consumer = consumer
        _background_thread = thread
        return consumer


def ensure_background_consumer_running(**kwargs) -> KafkaBatchConsumer:
    """Idempotently start the background consumer with optional overrides."""
    return start_background_consumer(**kwargs)


def stop_background_consumer() -> None:
    """Signal the background consumer to stop and wait for the thread to exit."""
    global _background_consumer, _background_thread

    with _background_lock:
        consumer = _background_consumer
        thread = _background_thread
        _background_consumer = None
        _background_thread = None

    if consumer:
        consumer.stop()
    if thread and thread.is_alive():
        thread.join(timeout=5)

# Ensure background consumer shuts down with the process
atexit.register(stop_background_consumer)
