

"""
WebSocket consumer for chat messages.

Behavior:
- On receive: parse JSON payload, validate minimal fields, push message to Redis (fast path)
  and broadcast to the Channel layer so connected websocket clients see the message immediately.
- Uses `push_message` from chat.redis_helper which returns a `temp_id`. Clients can use temp_id
  to reconcile optimistic UI updates with persisted DB rows later (when a flush worker persists
  messages and you optionally ACK them back to the client).
- Minimal assumptions about authentication are made; if an authenticated user is available in
  `self.scope['user']` we attach their id, otherwise the client may supply `sender_id`.
"""


import json
import logging
from typing import Optional
from uuid import uuid4

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer
from django.core.serializers.json import DjangoJSONEncoder

from chat.models import Conversation, ConversationParticipant, ConversationMessage
from django.contrib.auth import get_user_model
from django.utils import timezone

from chat.helpers.kafka_producer import fetch_messages_for_conversations, produce_message

logger = logging.getLogger(__name__)


class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # conversation_id is captured from the websocket URL route (see routing.py)
        user = self.scope.get("user")
        # Require authenticated user to connect (recommended)
        if user is None or not getattr(user, "is_authenticated", False):
            # Close with a specific code for unauthorized (4401 is custom here)
            await self.close(code=4401)
            return

        self.sender_id = int(getattr(user, "id", None))
        self.conversation_id = self.scope["url_route"]["kwargs"]["conversation_id"]
        self.room_group_name = f"chat_{self.conversation_id}"

        # Join the channel layer group for this conversation so we can broadcast
        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        # Leave the room group; don't re-check auth here (connection was accepted earlier)
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    async def receive(self, text_data: str):
        """
        Expected payload (JSON):
        {
            "message": "Hello",
            "message_type": "TEXT",    # optional, default "TEXT"
            "temp_id": "uuid-..."      # optional client-provided temporary id
        }
        """
        # Defensive check: ensure we have a sender_id resolved in connect
        if not hasattr(self, "sender_id") or self.sender_id is None:
            await self.send(text_data=json.dumps({"error": "authentication_required"}))
            await self.close(code=4401)
            return

        try:
            payload = json.loads(text_data)
        except (TypeError, ValueError):
            # Invalid JSON; reply and ignore
            await self.send(text_data=json.dumps({"error": "invalid_json"}))
            return

        message = payload.get("message")
        if message is None:
            await self.send(text_data=json.dumps({"error": "message_required"}))
            return

        # Use the authenticated sender id only. Do NOT accept sender_id from client.
        sender_id: Optional[int] = self.sender_id

        temp_id = payload.get("temp_id") or str(uuid4())
        kafka_payload = {
            "content": message,
            "sender_id": sender_id,
            "message_type": payload.get("message_type", "TEXT"),
            "temp_id": temp_id,
            "conversation_id": int(self.conversation_id),
            "reply_to_id": None,
        }

        # Produce to Kafka (fire-and-forget, best-effort)
        kafka_ok = False
        try:
            produce_message(self.conversation_id, kafka_payload, synchronous=False)
            kafka_ok = True
        except Exception as exc:
            logger.warning("Kafka produce failed for conv %s: %s", self.conversation_id, exc)

        # Always persist the message to DB so it survives page reloads
        try:
            db_message = await self._save_message_to_db(
                message, payload.get("message_type", "TEXT"), payload.get("reply_to_id")
            )
            logger.info(
                "Message saved to DB: id=%s conv=%s kafka_ok=%s",
                db_message.id, self.conversation_id, kafka_ok
            )
        except Exception as exc:
            logger.exception("Failed to save message to DB for conv %s: %s", self.conversation_id, exc)
            if not kafka_ok:
                await self.send(text_data=json.dumps({"error": "message_save_failed", "detail": str(exc)}))
                return

        # Resolve sender display name for immediate broadcast (so clients don't need to
        # look it up themselves and will show correct sender name in group chats).
        try:
            sender_obj = db_message.sender
            sender_name = (f"{sender_obj.first_name} {sender_obj.last_name}".strip() or sender_obj.username)
        except Exception:
            sender_name = "Unknown"

        # Broadcast to the group so all connected clients receive the new message immediately.
        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "chat.message",
                "message": message,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "temp_id": temp_id,
            },
        )

        # Broadcast to conversation-list WebSockets for all participants
        try:
            conv_info = await self._get_conversation_broadcast_info(
                message, payload.get("message_type", "TEXT")
            )
            
            logger.info(
                "Broadcasting conversation update: conv_id=%s, participants=%s, content='%s'",
                self.conversation_id,
                conv_info["participant_ids"],
                conv_info["conversation_data"]["latest_message"].get("content", "")[:50]
            )
            
            for user_id in conv_info["participant_ids"]:
                await self.channel_layer.group_send(
                    f"conversations_user_{user_id}",
                    {
                        "type": "conversation_update",
                        "action": "new_message",
                        "conversation": conv_info["conversation_data"],
                    },
                )
        except Exception as exc:
            logger.exception("Failed to broadcast conversation list update: %s", exc)

    @database_sync_to_async
    def _get_conversation_broadcast_info(self, message_content, message_type):
        """Get conversation participants and build the update payload.
        
        Uses the message content passed directly from receive() — no need to
        re-fetch from Kafka since we already have the data and it's been saved to DB.
        """
        User = get_user_model()
        conv_id = int(self.conversation_id)

        # Fetch minimal conversation metadata + participants from DB
        conv_meta = Conversation.objects.filter(id=conv_id).values("id", "is_group", "name").first()
        if not conv_meta:
            raise Conversation.DoesNotExist(f"Conversation {conv_id} not found")

        participant_ids = list(
            ConversationParticipant.objects.filter(conversation_id=conv_id)
            .values_list("user_id", flat=True)
        )

        # Resolve sender name
        try:
            sender = User.objects.get(id=self.sender_id)
            sender_name = (f"{sender.first_name} {sender.last_name}".strip() or sender.username)
        except User.DoesNotExist:
            sender_name = "Unknown"

        latest_message = {
            "content": message_content,
            "message_type": message_type,
            "sender_id": self.sender_id,
            "sender_name": sender_name,
            "created_at": timezone.now().isoformat(),
            "is_deleted": False,
        }

        return {
            "participant_ids": participant_ids,
            "conversation_data": {
                "id": conv_meta["id"],
                "is_group": conv_meta["is_group"],
                "name": conv_meta["name"],
                "latest_message": latest_message,
            },
        }

    async def chat_message(self, event):
        """
        Handler for messages sent to the channel layer group.
        Simply forwards the event payload to the websocket client as JSON.
        """
        out = {
            "message": event.get("message"),
            "sender_id": event.get("sender_id"),
            "sender_name": event.get("sender_name"),
            "temp_id": event.get("temp_id"),
        }
        await self.send(text_data=json.dumps(out))

    @database_sync_to_async
    def _save_message_to_db(self, content, message_type, reply_to_id=None):
        """Save a message directly to the database."""
        User = get_user_model()
        conversation = Conversation.objects.get(id=int(self.conversation_id))
        sender = User.objects.get(id=self.sender_id)

        reply_to = None
        if reply_to_id:
            reply_to = ConversationMessage.objects.filter(id=reply_to_id).first()

        msg = ConversationMessage(
            conversation=conversation,
            sender=sender,
            content=content,
            message_type=message_type or "TEXT",
            reply_to=reply_to,
        )
        msg.save()  # This also updates conversation.updated_at via the model's save()
        return msg


class ConversationsListConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for the conversations list.
    - On connect: sends the full list of conversations (DMs + groups).
    - Real-time: forwards conversation_update events (new messages, etc.).
    - Supports ``ping`` and ``refresh`` commands from the client.
    """

    async def connect(self):
        user = self.scope.get("user")
        if user is None or not getattr(user, "is_authenticated", False):
            await self.close(code=4401)
            return

        self.user_id = int(getattr(user, "id", None))
        self.user_group_name = f"conversations_user_{self.user_id}"

        await self.channel_layer.group_add(self.user_group_name, self.channel_name)
        await self.accept()

        await self._send_initial_conversations()

    async def disconnect(self, close_code):
        if hasattr(self, "user_group_name"):
            await self.channel_layer.group_discard(self.user_group_name, self.channel_name)

    async def receive(self, text_data: str):
        try:
            payload = json.loads(text_data)
        except (TypeError, ValueError):
            await self.send(text_data=json.dumps({"error": "invalid_json"}))
            return

        msg_type = payload.get("type")
        if msg_type == "ping":
            await self.send(text_data=json.dumps({"type": "pong"}))
        elif msg_type == "refresh":
            await self._send_initial_conversations()

    async def conversation_update(self, event):
        """Forward conversation updates to the WebSocket client."""
        await self.send(text_data=json.dumps({
            "type": "conversation_update",
            "action": event.get("action"),
            "conversation": event.get("conversation"),
        }))

    async def _send_initial_conversations(self):
        """Query and send all conversations for the connected user."""
        try:
            conversations_data = await self._get_user_conversations()
            await self.send(text_data=json.dumps(
                {
                    "type": "initial_conversations",
                    "conversations": conversations_data,
                },
                cls=DjangoJSONEncoder,
            ))
        except Exception as exc:
            logger.exception("Failed to send initial conversations: %s", exc)
            await self.send(text_data=json.dumps({"error": "failed_to_load_conversations"}))

    @database_sync_to_async
    def _get_user_conversations(self):
        """
        Fetch all conversations for the connected user, serialized identically
        to the REST ``ConversationViewSet.list`` endpoint, enriched with Kafka
        messages.
        """
        from chat.models import Conversation
        from chat.serializers import ConversationSerializer
        from django.contrib.auth import get_user_model

        User = get_user_model()
        user = User.objects.get(id=self.user_id)

        qs = (
            Conversation.objects.filter(participants__user=user)
            .select_related("created_by")
            .prefetch_related("participants__user", "messages__sender")
            .distinct()
        )

        class _MockRequest:
            def __init__(self, u):
                self.user = u

        serializer = ConversationSerializer(
            qs, many=True, context={"request": _MockRequest(user)}
        )
        data = serializer.data

        # Enrich with latest Kafka messages
        conv_ids = [str(c["id"]) for c in data if c.get("id") is not None]
        if conv_ids:
            try:
                kafka_map = fetch_messages_for_conversations(
                    conv_ids, max_messages_per_conversation=1
                )
                for conversation in data:
                    cid = str(conversation.get("id"))
                    kafka_messages = kafka_map.get(cid) or []
                    if not kafka_messages:
                        continue
                    latest_kafka = kafka_messages[-1]
                    current_latest = conversation.get("latest_message")
                    if current_latest is None or _is_kafka_newer(
                        latest_kafka, current_latest
                    ):
                        try:
                            sender = User.objects.get(
                                id=latest_kafka.get("sender_id")
                            )
                            sender_name = (
                                f"{sender.first_name} {sender.last_name}".strip()
                                or sender.username
                            )
                        except User.DoesNotExist:
                            sender_name = "Unknown"
                        conversation["latest_message"] = {
                            "content": latest_kafka.get("content", ""),
                            "message_type": latest_kafka.get("message_type", "TEXT"),
                            "sender_id": latest_kafka.get("sender_id"),
                            "sender_name": sender_name,
                            "created_at": latest_kafka.get("created_at"),
                            "is_deleted": False,
                        }
            except Exception as exc:
                logger.warning("Kafka enrichment failed: %s", exc)

        return data


def _is_kafka_newer(kafka_msg, db_msg):
    """Compare timestamps to determine if a Kafka message is newer."""
    from django.utils.dateparse import parse_datetime

    kafka_time_str = kafka_msg.get("created_at")
    if not kafka_time_str:
        return False
    try:
        kafka_time = (
            parse_datetime(kafka_time_str)
            if isinstance(kafka_time_str, str)
            else kafka_time_str
        )
        db_time_str = db_msg.get("created_at")
        db_time = (
            parse_datetime(db_time_str)
            if isinstance(db_time_str, str)
            else db_time_str
        )
        return kafka_time > db_time
    except Exception:
        return False


def broadcast_conversation_update(user_ids: list, conversation_data: dict, action: str = "new_message"):

    channel_layer = get_channel_layer()

    for user_id in user_ids:
        group_name = f"conversations_user_{user_id}"
        async_to_sync(channel_layer.group_send)(
            group_name,
            {
                "type": "conversation_update",
                "action" : action,
                "conversation" : conversation_data,
            }
        )
