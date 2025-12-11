import asyncio
import json
import random
from datetime import datetime
from typing import AsyncGenerator

from django.contrib.auth import get_user_model
from django.http import HttpRequest, HttpResponse, StreamingHttpResponse
from django.shortcuts import redirect, render

from . import models

User = get_user_model()

def lobby(request: HttpRequest) -> HttpResponse:
    if request.method == 'POST':
        username = request.POST.get('username')
        if username:
            request.session['username'] = username
        else:
            names = [
                "Horatio", "Benvolio", "Mercutio", "Lysander", "Demetrius", "Sebastian", "Orsino",
                "Malvolio", "Hero", "Bianca", "Gratiano", "Feste", "Antonio", "Lucius", "Puck", "Lucio",
                "Goneril", "Edgar", "Edmund", "Oswald"
            ]
            request.session['username'] = f"{random.choice(names)}-{hash(datetime.now().timestamp())}"

        return redirect('chat')
    else:
        return render(request, 'lobby.html')

def chat(request: HttpRequest) -> HttpResponse:
    if not request.session.get('username'):
        return redirect('lobby')
    return render(request, 'chat.html')

def create_message(request: HttpRequest) -> HttpResponse:
    content = request.POST.get("content")
    username = "admin"

    if not username:
        return HttpResponse(status=403)
    #author = User.objects.get_or_create(first_name="test1", last_name='user', username_=username, email="test1@mail.com", password="Admin@123")
    author = {
        "username" : "admin2",
        "password": "admin"
    }
    if content:
        models.ConversationMessage.objects.create(sender=author, content=content, conversation=1)
        return HttpResponse(status=201)
    else:
        return HttpResponse(status=200)

async def stream_chat_messages(request: HttpRequest) -> StreamingHttpResponse:
    """
    Streams chat messages to the client as we create messages.
    """
    async def event_stream():
        """
        We use this function to send a continuous stream of data
        to the connected clients.
        """
        async for message in get_existing_messages():
            yield message

        last_id = await get_last_message_id()

        # Continuously check for new messages
        while True:
            new_messages = models.Message.objects.filter(id__gt=last_id).order_by('created_at').values(
                'id', 'author__name', 'content'
            )
            async for message in new_messages:
                yield f"data: {json.dumps(message)}\n\n"
                last_id = message['id']
            await asyncio.sleep(0.1)  # Adjust sleep time as needed to reduce db queries.

    async def get_existing_messages() -> AsyncGenerator:
        messages = models.Message.objects.all().order_by('created_at').values(
            'id', 'author__name', 'content'
        )
        async for message in messages:
            yield f"data: {json.dumps(message)}\n\n"

    async def get_last_message_id() -> int:
        last_message = await models.Message.objects.all().alast()
        return last_message.id if last_message else 0

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')
