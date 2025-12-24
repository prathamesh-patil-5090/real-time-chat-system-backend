"""
Middleware that ensures the Kafka consumer responsible for persisting chat messages
is running in the background without requiring a manual management command.

Add `chat.middleware.KafkaConsumerBootMiddleware` to `MIDDLEWARE` (preferably near
the top) to have it kick in automatically.
"""

import os
import threading
from typing import Any, Dict, Optional

from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

from app.settings import  KAFKA_SERVER_URL
from chat.helpers.kafka_consumer import ensure_background_consumer_running


class KafkaConsumerBootMiddleware(MiddlewareMixin):
    _consumer_pid: Optional[int] = None
    _lock = threading.Lock()

    def _maybe_start_consumer(self) -> None:
        current_pid = os.getpid()
        if self.__class__._consumer_pid == current_pid:
            return

        with self.__class__._lock:
            if self.__class__._consumer_pid == current_pid:
                return

            config = getattr(settings, "CHAT_KAFKA_CONSUMER_CONFIG", {}) or {}
            ensure_background_consumer_running(
                topic=config.get("topic", "chat-messages"),
                group_id=config.get("group_id", "chat-consumer-group"),
                bootstrap_servers=config.get("bootstrap_servers", f"{KAFKA_SERVER_URL}"),
                batch_size=int(config.get("batch_size", 100)),
                max_interval_seconds=float(config.get("max_interval_seconds", 60.0)),
                poll_timeout=float(config.get("poll_timeout", 1.0)),
                consumer_config=config.get("consumer_config"),
            )
            self.__class__._consumer_pid = current_pid

    def __init__(self, get_response):
        super().__init__(get_response)
        self._maybe_start_consumer()

    def __call__(self, request):
        self._maybe_start_consumer()
        return self.get_response(request)
