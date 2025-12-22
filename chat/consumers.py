import json
from typing import Optional
from uuid import uuid4

from channels.generic.websocket import AsyncWebsocketConsumer

from chat.helpers.kafka_producer import produce_message

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

class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # conversation_id is captured from the websocket URL route (see routing.py)
        self.conversation_id = self.scope["url_route"]["kwargs"]["conversation_id"]
        self.room_group_name = f"chat_{self.conversation_id}"

        # Join the channel layer group for this conversation so we can broadcast
        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        # Leave the room group
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    async def receive(self, text_data: str):
        """
        Expected payload (JSON):
        {
            "message": "Hello",
            "message_type": "TEXT",    # optional, default "TEXT"
            "sender_id": 123,          # optional if self.scope['user'] exists
            "temp_id": "uuid-..."      # optional client-provided temporary id
        }
        """
        try:
            payload = json.loads(text_data)
        except (TypeError, ValueError):
            # Invalid JSON; ignore or optionally send error back
            await self.send(text_data=json.dumps({"error": "invalid_json"}))
            return

        message = payload.get("message")
        if message is None:
            await self.send(text_data=json.dumps({"error": "message_required"}))
            return

        # Prefer authenticated user id if present in scope, fall back to payload sender_id
        user = self.scope.get("user")
        sender_id: Optional[int] = None
        if user is not None and hasattr(user, "is_authenticated") and user.is_authenticated:
            try:
                sender_id = int(getattr(user, "id", None))
            except Exception:
                sender_id = None

        if sender_id is None:
            # fallback to any sender_id client included
            sender_id = payload.get("sender_id")

        temp_id = payload.get("temp_id") or str(uuid4())
        kafka_payload = {
            "content": message,
            "sender_id": sender_id,
            "message_type": payload.get("message_type", "TEXT"),
            "temp_id": temp_id,
            "conversation_id": int(self.conversation_id),
            "reply_to_id": None,
        }

        # Produce to Kafka (fire-and-forget; on_delivery is handled inside producer helper)
        try:
            produce_message(self.conversation_id, kafka_payload, synchronous=False)
        except Exception as exc:
            await self.send(text_data=json.dumps({"error": "kafka_produce_failed", "detail": str(exc)}))
            return

        # Broadcast to the group so all connected clients receive the new message immediately.
        # Include temp_id so clients can reconcile optimistic local messages with server ack.
        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "chat.message",  # this maps to `chat_message` handler below
                "message": message,
                "sender_id": sender_id,
                "temp_id": temp_id,
            },
        )

    async def chat_message(self, event):
        """
        Handler for messages sent to the channel layer group.
        Simply forwards the event payload to the websocket client as JSON.
        """
        out = {
            "message": event.get("message"),
            "sender_id": event.get("sender_id"),
            "temp_id": event.get("temp_id"),
        }
        await self.send(text_data=json.dumps(out))
