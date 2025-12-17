import json
import os
import uuid
from typing import Any, Dict, List

import redis

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(redis_url, decode_response=True)

def pending_key(conversation_id: str) -> str:
    return f"chat: {conversation_id}: pending_messages"

def push_messages(conversation_id: str, payload: Dict[str, Any]) -> str:
    temp_id = payload.get("temp_id") or str(uuid.uuid4())
    payload['temp_id'] = temp_id
    r.rpush(pending_key(conversation_id), json.dumps(payload))
    return temp_id

def fetch_pending(conversation_id: str) -> List[Dict[str, Any]]:
    raw = r.lrange(pending_key(conversation_id), 0, -1)
    return [json.loads(x) for x in raw]

def pop_all_pending(conversation_id: str) -> List[Dict[str, Any]]:
    pipe = r.pipeline()
    pipe.lrange(pending_key(conversation_id), 0, -1)
    pipe.delete(pending_key(conversation_id))
    raw, _ = pipe.execute()
    return [json.loads(x) for x in raw]
