

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from app.settings import BASE_DIR, KAFKA_SERVER_URL

logger = logging.getLogger(__name__)

# SSL certificate paths
cert_dir = BASE_DIR / "certificates"

# NOTE: Consider moving config to Django settings / env variables
_KAFKA_CONFIG = {
    "bootstrap.servers": f"{KAFKA_SERVER_URL}",
    "security.protocol": "SSL",
    "ssl.ca.location": str(cert_dir / "ca.pem"),
    "ssl.certificate.location": str(cert_dir / "service.cert"),
    "ssl.key.location": str(cert_dir / "service.key"),
}

_producer = Producer(_KAFKA_CONFIG)
_DEFAULT_TOPIC = "chat-messages"


def _delivery_callback(err, msg):
    if err is not None:
        logger.error(
            "Kafka delivery failed for topic=%s key=%s: %s", msg.topic(), msg.key(), err
        )
    else:
        logger.debug(
            "Kafka produced to topic=%s partition=%s offset=%s key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key(),
        )


def produce_message(
    conversation_id: str,
    payload: Dict[str, Any],
    topic: Optional[str] = None,
    synchronous: bool = False,
    timeout: float = 5.0,
) -> None:
    """
    Produce a JSON-serialized message to Kafka.

    - Uses `conversation_id` as the message key to preserve ordering per conversation (when using key-based partitioning).
    - `topic` defaults to `_DEFAULT_TOPIC`.
    - If `synchronous` is True, flush will block up to `timeout` seconds.
    """
    t = topic or _DEFAULT_TOPIC
    try:
        value = json.dumps(payload).encode("utf-8")
        key = str(conversation_id).encode("utf-8")

        _producer.produce(topic=t, value=value, key=key, on_delivery=_delivery_callback)
        _producer.poll(0)

        if synchronous:
            _producer.flush(timeout)
    except KafkaException as ke:
        logger.exception(
            "Kafka produce exception for conversation=%s: %s", conversation_id, ke
        )
        raise
    except BufferError:
        logger.exception(
            "Local producer queue is full while producing conversation=%s",
            conversation_id,
        )
        raise


def fetch_messages_from_kafka(
    conversation_id: str,
    topic: Optional[str] = None,
    max_messages: int = 100,
    timeout_seconds: float = 5.0,
    persist_group: Optional[str] = "chat-consumer-group",
) -> List[Dict[str, Any]]:
    """
    Fetch pending messages for a conversation from Kafka without committing offsets.

    This function returns only messages that are pending processing by the persistence
    consumer group (i.e., messages with offsets greater than the consumer group's
    committed offsets). It does this by:

    - Querying topic partitions and watermarks.
    - Querying the committed offsets for `persist_group`.
    - Reading messages whose offsets are > committed_offset for each partition and
      whose message key matches `conversation_id`.

    Caveats:
    - This performs broker metadata and offset lookups and a limited read. It is
      best-effort and intended for small-scale reads (similar role as Redis fetch_pending).
    - If your persistence consumer uses a different group id, pass it via `persist_group`.
    - The function does NOT commit any offsets for any group.
    """
    t = topic or _DEFAULT_TOPIC
    results: List[Dict[str, Any]] = []
    if max_messages <= 0:
        return results

    bootstrap_servers = _KAFKA_CONFIG.get("bootstrap.servers")

    # Temporary consumer for reading messages (unique group so we don't affect others)
    tmp_group = f"fetch-{uuid.uuid4()}"
    read_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": tmp_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "security.protocol": "SSL",
        "ssl.ca.location": str(cert_dir / "ca.pem"),
        "ssl.certificate.location": str(cert_dir / "service.cert"),
        "ssl.key.location": str(cert_dir / "service.key"),
    }

    # Consumer used only to query committed offsets for the persistence group
    commit_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": persist_group or "chat-consumer-group",
        "enable.auto.commit": False,
        "security.protocol": "SSL",
        "ssl.ca.location": str(cert_dir / "ca.pem"),
        "ssl.certificate.location": str(cert_dir / "service.cert"),
        "ssl.key.location": str(cert_dir / "service.key"),
        # don't subscribe/assign this consumer; we will only use committed()
    }

    reader = Consumer(read_conf)
    comm_consumer = Consumer(commit_conf)

    try:
        md = reader.list_topics(topic=t, timeout=5.0)
        if t not in md.topics:
            logger.warning("Topic %s not found when fetching pending messages", t)
            return results

        partitions = list(md.topics[t].partitions.keys())
        if not partitions:
            return results

        # Determine start offsets for each partition based on committed offsets of persist_group
        topic_partitions = []
        for p in partitions:
            tp = TopicPartition(t, p)
            try:
                low, high = reader.get_watermark_offsets(tp, cached=False)
            except Exception:
                try:
                    low = 0
                    high = reader.get_watermark_offsets(TopicPartition(t, p))[1]
                except Exception:
                    low, high = 0, 0

            # Query committed offset for persistence group
            try:
                committed = comm_consumer.committed([TopicPartition(t, p)], timeout=5.0)
                if committed and len(committed) > 0:
                    committed_off = committed[0].offset
                    if committed_off is None or committed_off < 0:
                        # no committed offset -> treat as nothing processed
                        committed_off = low - 1
                else:
                    committed_off = low - 1
            except Exception:
                # If we can't fetch committed offsets, assume none committed so start at low
                committed_off = low - 1

            # pending messages start at committed_off + 1
            start = max(low, committed_off + 1)
            # if start >= high there are no pending messages in this partition
            if start < high:
                topic_partitions.append(TopicPartition(t, p, start))

        if not topic_partitions:
            return results

        # assign reader to the computed starting offsets
        reader.assign(topic_partitions)

        deadline = time.time() + float(timeout_seconds)
        while time.time() < deadline and len(results) < max_messages:
            msg = reader.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.debug("Kafka poll returned error while fetching pending messages: %s", msg.error())
                continue

            try:
                # Ensure key matches conversation_id
                raw_key = msg.key()
                if raw_key is None:
                    continue
                key_str = raw_key.decode("utf-8") if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                if key_str != str(conversation_id):
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    continue

                payload = (
                    json.loads(raw_value.decode("utf-8"))
                    if isinstance(raw_value, (bytes, bytearray))
                    else json.loads(str(raw_value))
                )

                payload_meta = {
                    "_kafka_topic": msg.topic(),
                    "_kafka_partition": msg.partition(),
                    "_kafka_offset": msg.offset(),
                }
                if isinstance(payload, dict):
                    payload.update(payload_meta)
                    results.append(payload)
                else:
                    results.append({"value": payload, **payload_meta})
            except Exception:
                logger.exception("Failed to decode/filter kafka pending message: %r", msg)
                continue

    except Exception:
        logger.exception("Failed fetching pending messages from kafka for conversation=%s", conversation_id)
    finally:
        try:
            reader.close()
        except Exception:
            logger.exception("Failed to close temporary reader consumer")
        try:
            comm_consumer.close()
        except Exception:
            logger.exception("Failed to close commit-offset consumer")

    return results[:max_messages]


def fetch_messages_for_conversations(
    conversation_ids: List[str],
    topic: Optional[str] = None,
    max_messages_per_conversation: int = 100,
    timeout_seconds: float = 5.0,
    persist_group: Optional[str] = "chat-consumer-group",
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch pending messages for multiple conversations in a single broker round-trip.

    Returns a mapping of conversation_id -> list of message payloads. This reduces
    expensive metadata/offset lookups and polls when you need pending messages for
    many conversations at once (e.g. for paginated conversation lists).
    """
    t = topic or _DEFAULT_TOPIC
    out: Dict[str, List[Dict[str, Any]]] = {str(cid): [] for cid in conversation_ids}
    if not conversation_ids:
        return out

    bootstrap_servers = _KAFKA_CONFIG.get("bootstrap.servers")

    tmp_group = f"fetch-{uuid.uuid4()}"
    read_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": tmp_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "security.protocol": "SSL",
        "ssl.ca.location": str(cert_dir / "ca.pem"),
        "ssl.certificate.location": str(cert_dir / "service.cert"),
        "ssl.key.location": str(cert_dir / "service.key"),
    }

    commit_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": persist_group or "chat-consumer-group",
        "enable.auto.commit": False,
        "security.protocol": "SSL",
        "ssl.ca.location": str(cert_dir / "ca.pem"),
        "ssl.certificate.location": str(cert_dir / "service.cert"),
        "ssl.key.location": str(cert_dir / "service.key"),
    }

    reader = Consumer(read_conf)
    comm_consumer = Consumer(commit_conf)

    try:
        md = reader.list_topics(topic=t, timeout=5.0)
        if t not in md.topics:
            logger.warning("Topic %s not found when fetching pending messages", t)
            return out

        partitions = list(md.topics[t].partitions.keys())
        if not partitions:
            return out

        topic_partitions = []
        for p in partitions:
            tp = TopicPartition(t, p)
            try:
                low, high = reader.get_watermark_offsets(tp, cached=False)
            except Exception:
                try:
                    low = 0
                    high = reader.get_watermark_offsets(TopicPartition(t, p))[1]
                except Exception:
                    low, high = 0, 0

            try:
                committed = comm_consumer.committed([TopicPartition(t, p)], timeout=5.0)
                if committed and len(committed) > 0:
                    committed_off = committed[0].offset
                    if committed_off is None or committed_off < 0:
                        committed_off = low - 1
                else:
                    committed_off = low - 1
            except Exception:
                committed_off = low - 1

            start = max(low, committed_off + 1)
            if start < high:
                topic_partitions.append(TopicPartition(t, p, start))

        if not topic_partitions:
            return out

        reader.assign(topic_partitions)

        deadline = time.time() + float(timeout_seconds)
        needed = {str(cid): max_messages_per_conversation for cid in conversation_ids}

        while time.time() < deadline and any(len(out[cid]) < needed[cid] for cid in out):
            msg = reader.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.debug("Kafka poll returned error while fetching pending messages: %s", msg.error())
                continue

            try:
                raw_key = msg.key()
                if raw_key is None:
                    continue
                key_str = raw_key.decode("utf-8") if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                if key_str not in out:
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    continue

                payload = (
                    json.loads(raw_value.decode("utf-8"))
                    if isinstance(raw_value, (bytes, bytearray))
                    else json.loads(str(raw_value))
                )

                payload_meta = {
                    "_kafka_topic": msg.topic(),
                    "_kafka_partition": msg.partition(),
                    "_kafka_offset": msg.offset(),
                }

                if isinstance(payload, dict):
                    payload.update(payload_meta)
                    if len(out[key_str]) < needed[key_str]:
                        out[key_str].append(payload)
                else:
                    if len(out[key_str]) < needed[key_str]:
                        out[key_str].append({"value": payload, **payload_meta})
            except Exception:
                logger.exception("Failed to decode/filter kafka pending message: %r", msg)
                continue

    except Exception:
        logger.exception("Failed fetching pending messages from kafka for conversations=%s", conversation_ids)
    finally:
        try:
            reader.close()
        except Exception:
            logger.exception("Failed to close temporary reader consumer")
        try:
            comm_consumer.close()
        except Exception:
            logger.exception("Failed to close commit-offset consumer")

    # Trim to per-conversation limits and return
    return {k: v[:max_messages_per_conversation] for k, v in out.items()}

def produce_message_update(
    conversation_id: str,
    message_id: int,
    payload: Dict[str, Any],
    topic: Optional[str] = None,
    synchronous: bool = False,
    timeout: float = 5.0,
) -> None:
    """
    Produce a message update event to Kafka.
    The payload should include the updated fields and an action_type of 'update'.
    """
    t = topic or _DEFAULT_TOPIC
    update_payload = {
        **payload,
        "message_id": message_id,
        "action_type": "update",
        "conversation_id": conversation_id,
    }
    produce_message(conversation_id, update_payload, topic=t, synchronous=synchronous, timeout=timeout)


def produce_message_delete(
    conversation_id: str,
    message_id: int,
    topic: Optional[str] = None,
    synchronous: bool = False,
    timeout: float = 5.0,
) -> None:
    """
    Produce a message delete event to Kafka.
    """
    t = topic or _DEFAULT_TOPIC
    delete_payload = {
        "message_id": message_id,
        "action_type": "delete",
        "conversation_id": conversation_id,
        "is_deleted": True,
    }
    produce_message(conversation_id, delete_payload, topic=t, synchronous=synchronous, timeout=timeout)


def find_message_in_kafka(
    conversation_id: str,
    message_id: int,
    topic: Optional[str] = None,
    timeout_seconds: float = 5.0,
    persist_group: Optional[str] = "chat-consumer-group",
) -> Optional[Dict[str, Any]]:
    """
    Search for a specific message in Kafka pending messages by message_id.
    Returns the message payload if found, None otherwise.

    Note: This searches through pending messages. The message must have been
    created with a message_id field in its payload.

    Args:
        conversation_id: The conversation ID to search in
        message_id: The ID of the message to find
        topic: Kafka topic (defaults to _DEFAULT_TOPIC)
        timeout_seconds: How long to search before giving up
        persist_group: Consumer group used for persistence

    Returns:
        The message payload dict if found, None otherwise
    """
    pending_messages = fetch_messages_from_kafka(
        conversation_id=str(conversation_id),
        topic=topic,
        max_messages=1000,  # Increase if you expect more pending messages
        timeout_seconds=timeout_seconds,
        persist_group=persist_group,
    )

    # Search for the message by ID
    for msg in pending_messages:
        # Check if this message matches by ID
        if msg.get("message_id") == message_id:
            return msg
        # Also check the Kafka offset metadata if the message_id wasn't set
        # (for backwards compatibility with older messages)
        if msg.get("id") == message_id:
            return msg

    return None
