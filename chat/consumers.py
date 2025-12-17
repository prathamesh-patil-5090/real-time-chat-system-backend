import json

from channels.generic.websocket import AsyncWebsocketConsumer

from chat.redis_helper import push_messages


class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.conversation_id = self.scope["url_route"]["kwargs"]["conversation_id"]
        self.room_group_name = f"chat_{self.conversation_id}"
        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    async def receive(self, text_data):
        payload = json.load(text_data)
        message = payload.get("message")
        sender_id = payload.get("sender_id")
        cached_payload = {
            "content" : message,
            "sender_id" : sender_id,
            "message_type": payload.get("message_type", "TEXT")
        }

        push_messages(self.conversation_id, cached_payload)

        await self.channel_layer.group_send(
            self.room_group_name, {"type": "chat.message", "message": message, "sender_id" : sender_id},
        )

    async def chat_message(self, event):
        message = event["message"]
        await self.send(text_data=json.dumps({
            "message": message,
            "sender_id" : event.get("sender_id"),
        }))
