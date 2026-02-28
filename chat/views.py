from typing import List, Optional
from uuid import uuid4

from authentication.authentication import CookieJWTAuthentication
from chat.helpers.kafka_producer import (
    fetch_messages_for_conversations,
    fetch_messages_from_kafka,
    find_message_in_kafka,
    produce_message,
    produce_message_delete,
    produce_message_update,
)
from chat.models import (
    Conversation,
    ConversationMessage,
    ConversationParticipant,
    MessageReadReceipt,
    RoleEnum,
)
from chat.serializers import (
    ConversationMessageSerializer,
    ConversationParticipantsSerializer,
    ConversationSerializer,
    MessageReadReceiptsSerializer,
    SetPagination,
)
from django.contrib.auth import get_user_model
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import permissions, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

User = get_user_model()


#
# ConversationViewSet
# - API-only viewset to manage conversations and related actions (participants, messages, read receipts).
# - Uses CookieJWTAuthentication and requires authentication for all actions.
# - Message creation writes to Redis (fast path) via `push_message`. A background flusher should persist Redis -> DB.
#
class ConversationViewSet(viewsets.ModelViewSet):
    serializer_class = ConversationSerializer
    authentication_classes = [CookieJWTAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        """
        Restrict conversations to those the requesting user participates in.
        Optimize with prefetch_related to avoid N+1 queries.
        """
        user = self.request.user

        qs = Conversation.objects.filter(participants__user=user).select_related("created_by").prefetch_related(
            "participants__user",
            "messages__sender",
        ).distinct()

        is_group_param = None
        try:
            is_group_param = self.request.query_params.get("is_group")
        except Exception:
            is_group_param = None

        if is_group_param is not None:
            val = str(is_group_param).lower()
            if val in ("1", "true", "t", "yes", "y"):
                qs = qs.filter(is_group=True)
            elif val in ("0", "false", "f", "no", "n"):
                qs = qs.filter(is_group=False)

        return qs

    def list(self, request, *args, **kwargs):
        """
        Override list to enrich conversations with latest Kafka messages.
        """
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            data = serializer.data
            data = self._enrich_with_kafka_messages(data)
            return self.get_paginated_response(data)

        serializer = self.get_serializer(queryset, many=True)
        data = serializer.data
        data = self._enrich_with_kafka_messages(data)
        return Response(data)

    def _enrich_with_kafka_messages(self, conversations_data):
        """
        Check Kafka for each conversation and update latest_message if a newer message exists.
        """
        conv_ids = [str(c['id']) for c in conversations_data if c.get('id') is not None]
        if not conv_ids:
            return conversations_data

        kafka_map = fetch_messages_for_conversations(conv_ids, max_messages_per_conversation=1)

        for conversation in conversations_data:
            cid = str(conversation.get('id'))
            kafka_messages = kafka_map.get(cid) or []
            if not kafka_messages:
                continue

            latest_kafka_msg = kafka_messages[-1]
            current_latest = conversation.get('latest_message')
            if current_latest is None or self._is_kafka_message_newer(latest_kafka_msg, current_latest):
                try:
                    sender = User.objects.get(id=latest_kafka_msg.get('sender_id'))
                    sender_name = f"{sender.first_name} {sender.last_name}".strip() or sender.username
                except User.DoesNotExist:
                    sender_name = "Unknown"

                conversation['latest_message'] = {
                    'content': latest_kafka_msg.get('content', ''),
                    'message_type': latest_kafka_msg.get('message_type', 'TEXT'),
                    'sender_id': latest_kafka_msg.get('sender_id'),
                    'sender_name': sender_name,
                    'created_at': latest_kafka_msg.get('created_at'),
                    'is_deleted': False,
                    'from_kafka': True,
                }

        return conversations_data

    def _is_kafka_message_newer(self, kafka_msg, db_msg):
        """
        Compare timestamps to determine if kafka message is newer.
        """
        from datetime import datetime

        from django.utils.dateparse import parse_datetime

        kafka_time_str = kafka_msg.get('created_at')
        if not kafka_time_str:
            return False

        try:
            if isinstance(kafka_time_str, str):
                kafka_time = parse_datetime(kafka_time_str)
            else:
                kafka_time = kafka_time_str

            db_time_str = db_msg.get('created_at')
            if isinstance(db_time_str, str):
                db_time = parse_datetime(db_time_str)
            else:
                db_time = db_time_str

            return kafka_time > db_time
        except:
            return False

    def perform_create(self, serializer):
        """
        On creation, set created_by via serializer (ConversationSerializer does this),
        then add the creator as an ADMIN participant. Optionally accept `participant_ids`
        in the request data to add other users as members.

        For personal chats (is_group=False), ensure exactly 2 participants total.
        """
        is_group = serializer.validated_data.get('is_group', False)
        extra_participant_ids = self._normalize_id_list(self.request.data.get("participant_ids", []))

        if not is_group:
            if len(extra_participant_ids) != 1:
                raise ValidationError({"participant_ids": "Personal chats must have exactly one other participant."})

            other_user_id = extra_participant_ids[0]
            existing = Conversation.objects.filter(
                is_group=False,
                participants__user=self.request.user
            ).filter(
                participants__user_id=other_user_id
            ).distinct().first()

            if existing:
                raise ValidationError({
                    "detail": "A personal conversation with this user already exists.",
                    "conversation_id": existing.id
                })

        conversation = serializer.save()
        ConversationParticipant.objects.get_or_create(
            conversation=conversation,
            user=self.request.user,
            defaults={"role": RoleEnum.ADMIN},
        )

        for user_id in extra_participant_ids:
            if str(user_id) == str(self.request.user.id):
                continue
            ConversationParticipant.objects.get_or_create(
                conversation=conversation,
                user_id=user_id,
                defaults={"role": RoleEnum.MEMBER if is_group else RoleEnum.ADMIN},
            )

    def perform_update(self, serializer):
        """
        Only an admin may update conversation metadata.
        """
        conversation = self.get_object()
        if not self._is_admin(conversation, self.request.user):
            raise PermissionDenied("Only conversation admins can update conversation settings.")
        serializer.save()

    def destroy(self, request, *args, **kwargs):
        """
        Only an admin may delete a conversation.
        """
        conversation = self.get_object()
        if not self._is_admin(conversation, request.user):
            raise PermissionDenied("Only conversation admins can delete a conversation.")
        return super().destroy(request, *args, **kwargs)

    # ------------------------
    # Participants endpoints
    # ------------------------
    @action(detail=True, methods=["get", "post"], url_path="participants")
    def participants(self, request, pk=None):
        """
        Combined participants endpoint:
         - GET: list participants for a conversation.
         - POST: add a participant (admin only) and expects `user_id` in the body.
        """
        conversation = self.get_object()

        if request.method.lower() == "get":
            participants = ConversationParticipant.objects.filter(conversation=conversation).select_related("user")
            data = ConversationParticipantsSerializer(participants, many=True).data
            return Response(data)

        if not self._is_admin(conversation, request.user):
            raise PermissionDenied("Only conversation admins can add participants.")
        user_id = request.data.get("user_id")
        if not user_id:
            raise ValidationError("user_id is required.")

        participant, created = ConversationParticipant.objects.get_or_create(
            conversation=conversation,
            user_id=user_id,
            defaults={"role": RoleEnum.MEMBER},
        )
        serializer = ConversationParticipantsSerializer(participant)
        return Response(serializer.data, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)

    @action(detail=True, methods=["delete"], url_path=r"participants/(?P<user_id>[^/.]+)")
    def remove_participant(self, request, pk=None, user_id=None):
        """
        Remove a participant (path parameter `user_id`).
        Rules:
         - Requester must be an admin.
         - If the target is an ADMIN:
             * a newer admin (joined later) may not remove an older admin.
             * you may not remove the last admin from the conversation.
        """
        conversation = self.get_object()
        if not self._is_admin(conversation, request.user):
            raise PermissionDenied("Only conversation admins can remove participants.")

        participant = get_object_or_404(ConversationParticipant, conversation=conversation, user_id=user_id)

        if participant.role == RoleEnum.ADMIN:
            requester_participant = ConversationParticipant.objects.filter(conversation=conversation, user=request.user).first()
            if requester_participant is None:
                raise PermissionDenied("Requester is not a participant in this conversation.")

            if requester_participant.joined_at and participant.joined_at:
                if requester_participant.joined_at > participant.joined_at:
                    raise ValidationError("A newer admin cannot remove an older admin.")

            admin_count = ConversationParticipant.objects.filter(conversation=conversation, role=RoleEnum.ADMIN).count()
            if admin_count <= 1:
                raise ValidationError("Cannot remove the last admin from the conversation.")

        participant.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    # ------------------------
    # Messages endpoints
    # ------------------------
    @action(detail=True, methods=["get", "post"], url_path="messages")
    def messages(self, request, pk=None):
        conversation = self.get_object()
        self._ensure_participant(conversation, request.user)

        if request.method.lower() == "get":
            db_qs = ConversationMessage.objects.filter(
                conversation=conversation,
                is_deleted=False
            ).select_related("sender").order_by("-created_at")

            paginator = SetPagination()
            page = paginator.paginate_queryset(db_qs, request)
            db_data = ConversationMessageSerializer(
                page,
                many=True,
                context={"request": request, "conversation": conversation},
            ).data

            pending_list = fetch_messages_from_kafka(conversation.id)

            sender_ids = {msg.get('sender_id') for msg in pending_list if msg.get('sender_id')}
            senders_map = {}
            if sender_ids:
                senders = User.objects.filter(id__in=sender_ids).only(
                    'id', 'username', 'first_name', 'last_name', 'email'
                )
                senders_map = {user.id: user for user in senders}

            count_of_persisted_data = len(db_data)
            enriched_pending = []

            for i, pending_msg in enumerate(pending_list):
                sender_id = pending_msg.get('sender_id')
                sender_obj = senders_map.get(sender_id)

                enriched_msg = {
                    'id': count_of_persisted_data + i + 1,
                    'conversation': conversation.id,
                    'sender': {
                        'id': sender_obj.id if sender_obj else sender_id,
                        'username': sender_obj.username if sender_obj else f'user_{sender_id}',
                        'first_name': sender_obj.first_name if sender_obj else '',
                        'last_name': sender_obj.last_name if sender_obj else '',
                        'email': sender_obj.email if sender_obj else '',
                    } if sender_obj else {
                        'id': sender_id,
                        'username': f'user_{sender_id}',
                        'first_name': '',
                        'last_name': '',
                        'email': '',
                    },
                    'content': pending_msg.get('content', ''),
                    'message_type': pending_msg.get('message_type', 'TEXT'),
                    'reply_to': pending_msg.get('reply_to_id'),
                    'is_deleted': pending_msg.get('is_deleted', False),
                    'created_at': pending_msg.get('created_at'),
                    'updated_at': pending_msg.get('created_at'),
                }
                enriched_pending.append(enriched_msg)

            all_messages = db_data + enriched_pending

            return paginator.get_paginated_response({"messages": all_messages})

        serializer = ConversationMessageSerializer(
            data=request.data,
            context={"request": request, "conversation": conversation},
        )
        serializer.is_valid(raise_exception=True)

        payload = {
            "content": serializer.validated_data.get("content"),
            "message_type": serializer.validated_data.get("message_type", "TEXT"),
            "sender_id": request.user.id,
            "conversation_id": conversation.id,
            "reply_to_id": getattr(serializer.validated_data.get("reply_to"), "id", None),
            "created_at": timezone.now().isoformat(),
        }
        temp_id = uuid4()
        produce_message(conversation.id, payload)
        return Response(
            {"status": "queued", "temp_id": temp_id, "payload": payload},
            status=status.HTTP_202_ACCEPTED
        )

    @action(detail=True, methods=["PUT", "PATCH", "DELETE"], url_path=r"messages/(?P<message_id>[^/.]+)")
    def manage_message(self, request, pk=None, message_id=None):
        """
        Manage individual messages (update or delete).
        - PUT/PATCH: Update a message (sender only)
        - DELETE: Delete a message (sender or admin)
        Handles both DB-persisted messages and Kafka-pending messages.
        """
        conversation = self.get_object()
        self._ensure_participant(conversation, request.user)

        # Handle DELETE request
        if request.method == "DELETE":
            try:
                message = ConversationMessage.objects.get(
                    pk=message_id,
                    conversation=conversation,
                )

                is_sender = message.sender == request.user
                is_admin = self._is_admin(conversation, request.user)

                if not (is_sender or is_admin):
                    raise PermissionDenied("You can only delete your own messages or you must be an admin.")

                if message.is_deleted:
                    raise ValidationError("Message is already deleted")

                message.is_deleted = True
                message.save()

                return Response(
                    {"detail": "Message deleted successfully."},
                    status=status.HTTP_200_OK
                )

            except ConversationMessage.DoesNotExist:
                kafka_msg = find_message_in_kafka(str(conversation.id), int(message_id))

                if kafka_msg:
                    is_sender = kafka_msg.get('sender_id') == request.user.id
                    is_admin = self._is_admin(conversation, request.user)

                    if not (is_sender or is_admin):
                        raise PermissionDenied("You can only delete your own messages or you must be an admin.")

                    produce_message_delete(
                        conversation_id=str(conversation.id),
                        message_id=int(message_id)
                    )

                    return Response(
                        {
                            "status": "queued",
                            "message": "Message deletion queued. The message is still being processed."
                        },
                        status=status.HTTP_202_ACCEPTED
                    )
                else:
                    return Response(
                        {"detail": "Message not found."},
                        status=status.HTTP_404_NOT_FOUND
                    )

        else:
            try:
                message = ConversationMessage.objects.get(pk=message_id, conversation=conversation)

                if message.sender != request.user:
                    raise PermissionDenied("You can only update your own messages.")

                if message.is_deleted:
                    raise ValidationError("Cannot update a deleted message.")

                partial = request.method == "PATCH"
                serializer = ConversationMessageSerializer(
                    message,
                    data=request.data,
                    partial=partial,
                    context={"request": request, "conversation": conversation}
                )
                serializer.is_valid(raise_exception=True)
                serializer.save()

                return Response(serializer.data, status=status.HTTP_200_OK)

            except ConversationMessage.DoesNotExist:
                kafka_msg = find_message_in_kafka(str(conversation.id), int(message_id))

                if kafka_msg:
                    if kafka_msg.get('sender_id') != request.user.id:
                        raise PermissionDenied("You can only update your own messages.")

                    update_payload = {
                        "content": request.data.get("content", kafka_msg.get("content")),
                        "message_type": request.data.get("message_type", kafka_msg.get("message_type", "TEXT")),
                        "sender_id": request.user.id,
                        "updated_at": timezone.now().isoformat(),
                    }

                    produce_message_update(
                        conversation_id=str(conversation.id),
                        message_id=int(message_id),
                        payload=update_payload
                    )

                    return Response(
                        {
                            "status": "queued",
                            "message": "Message update queued. The message is still being processed.",
                            "payload": update_payload
                        },
                        status=status.HTTP_202_ACCEPTED
                    )
                else:
                    return Response(
                        {"detail": "Message not found."},
                        status=status.HTTP_404_NOT_FOUND
                    )

    @action(detail=True, methods=["post"], url_path=r"mark-read/(?P<message_id>[^/.]+)")
    def mark_read(self, request, pk=None, message_id=None):
        """
        Mark a given message as read by the current user and update the participant's last_read_message pointer.
        """
        conversation = self.get_object()
        self._ensure_participant(conversation, request.user)

        message = get_object_or_404(ConversationMessage, pk=message_id, conversation=conversation)
        receipt, _ = MessageReadReceipt.objects.get_or_create(message=message, user=request.user)
        ConversationParticipant.objects.filter(conversation=conversation, user=request.user).update(last_read_message=message)
        data = MessageReadReceiptsSerializer(receipt).data
        return Response(data, status=status.HTTP_200_OK)

    # ------------------------
    # Internal helpers
    # ------------------------
    def _is_admin(self, conversation: Conversation, user: User) -> bool:
        return ConversationParticipant.objects.filter(conversation=conversation, user=user, role=RoleEnum.ADMIN).exists()

    def _ensure_participant(self, conversation: Conversation, user: User) -> None:
        if not ConversationParticipant.objects.filter(conversation=conversation, user=user).exists():
            raise PermissionDenied("You are not a participant in this conversation.")

    def _normalize_id_list(self, ids: Optional[List[int]]) -> List[int]:
        """
        Accept list or comma-separated string and return a list of ids.
        """
        if ids is None:
            return []
        if isinstance(ids, str):
            return [int(i.strip()) for i in ids.split(",") if i.strip()]
        if isinstance(ids, (list, tuple)):
            out = []
            for i in ids:
                try:
                    out.append(int(i))
                except Exception:
                    continue
            return out
        return []


#
# ConversationParticipantViewSet
# - Basic CRUD for participants scoped by ?conversation_id
#
class ConversationParticipantViewSet(viewsets.ModelViewSet):
    serializer_class = ConversationParticipantsSerializer
    authentication_classes = [CookieJWTAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        conversation_id = self.request.query_params.get("conversation_id")
        if not conversation_id:
            return ConversationParticipant.objects.none()
        self._ensure_participant(conversation_id, self.request.user)
        return ConversationParticipant.objects.filter(conversation_id=conversation_id).select_related("user")

    def perform_create(self, serializer):
        conversation_id = self.request.query_params.get("conversation_id") or self.request.data.get("conversation")
        if not conversation_id:
            raise ValidationError("conversation_id (or conversation) is required.")
        conversation = get_object_or_404(Conversation, pk=conversation_id)
        self._ensure_admin(conversation, self.request.user)
        user_id = self.request.data.get("user")
        if not user_id:
            raise ValidationError("user is required.")
        serializer.save(conversation=conversation, user_id=user_id)

    def perform_update(self, serializer):
        participant = self.get_object()
        self._ensure_admin(participant.conversation, self.request.user)
        serializer.save()

    def destroy(self, request, *args, **kwargs):
        participant = self.get_object()
        self._ensure_admin(participant.conversation, request.user)
        return super().destroy(request, *args, **kwargs)

    def _ensure_participant(self, conversation_id: int, user: User) -> None:
        if not ConversationParticipant.objects.filter(conversation_id=conversation_id, user=user).exists():
            raise PermissionDenied("You are not a participant in this conversation.")

    def _ensure_admin(self, conversation: Conversation, user: User) -> None:
        if not ConversationParticipant.objects.filter(conversation=conversation, user=user, role=RoleEnum.ADMIN).exists():
            raise PermissionDenied("Only admins can manage participants for this conversation.")


#
# MessageReadReceiptViewSet
#
class MessageReadReceiptViewSet(viewsets.ModelViewSet):
    serializer_class = MessageReadReceiptsSerializer
    authentication_classes = [CookieJWTAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        message_id = self.request.query_params.get("message_id")
        qs = MessageReadReceipt.objects.all().select_related("message", "user")
        if message_id:
            qs = qs.filter(message_id=message_id)
        return qs.filter(message__conversation__participants__user=self.request.user).distinct()

    def perform_create(self, serializer):
        message_id = self.request.data.get("message")
        if not message_id:
            raise ValidationError("message is required.")
        message = get_object_or_404(ConversationMessage, pk=message_id)
        if not ConversationParticipant.objects.filter(conversation_id=message.conversation_id, user=self.request.user).exists():
            raise PermissionDenied("You are not a participant in this conversation.")
        serializer.save(user=self.request.user)
