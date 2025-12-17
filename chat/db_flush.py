
from django.core.management.base import BaseCommand
from django.db import transaction
from models import ConversationMessage
from redis_helper import pop_all_pending, r


class Command(BaseCommand):
    help = "Flush pending chat messages from Redis into the database."

    def add_arguments(self, parser):
        parser.add_argument("--conversation_id", help="Flush only one conversation", default=None)

    def handle(self, *args, **options):
        convo_id = options.get("conversation_id")
        convo_ids = [convo_id] if convo_id else self._list_conversations_with_pending()

        for cid in convo_ids:
            self.stdout.write(f"Flushing conversation {cid} ...")
            pending = pop_all_pending()
            if not pending:
                continue
            with transaction.atomic():
                objs = []
                for p in pending:
                    objs.append(
                        ConversationMessage(
                            conversation_id=cid,
                            sender_id=p["sender_id"],
                            content = p.get("content", ""),
                            message_type = p.get("message_type", "TEXT"),
                            reply_to_id = p.get("reply_to_id"),
                        )
                    )
                ConversationMessage.objects.bulk_create(objs, ignore_conflicts=True)
                self.stdout.write(f"Persisted {len(pending)} messages for conversation {cid}")

    def _list_conversations_with_pending(self):
        keys = r.scan_iter(match="chat:*:pending_messages")
        convo_ids = []
        for k in keys:
            try:
                convo_ids.append(k.split(":")[1])
            except Exception:
                continue
        return convo_ids
