from typing import List, Optional
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.shortcuts import get_object_or_404
from rest_framework import permissions, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from authentication.authentication import CookieJWTAuthentication
from chat.helpers.kafka_producer import fetch_messages_from_kafka, produce_message
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
        """
        user = self.request.user
        return Conversation.objects.filter(participants__user=user).select_related("created_by").distinct()

    def perform_create(self, serializer):
        """
        On creation, set created_by via serializer (ConversationSerializer does this),
        then add the creator as an ADMIN participant. Optionally accept `participant_ids`
        in the request data to add other users as members.
        """
        conversation = serializer.save()
        ConversationParticipant.objects.get_or_create(
            conversation=conversation,
            user=self.request.user,
            defaults={"role": RoleEnum.ADMIN},
        )

        extra_participant_ids = self._normalize_id_list(self.request.data.get("participant_ids", []))
        for user_id in extra_participant_ids:
            # skip adding the creator again
            if str(user_id) == str(self.request.user.id):
                continue
            ConversationParticipant.objects.get_or_create(
                conversation=conversation,
                user_id=user_id,
                defaults={"role": RoleEnum.MEMBER},
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
            # Fetch requester's participant record to compare joined_at
            requester_participant = ConversationParticipant.objects.filter(conversation=conversation, user=request.user).first()
            if requester_participant is None:
                raise PermissionDenied("Requester is not a participant in this conversation.")

            # If both have join timestamps, prevent newer admin from removing an older admin
            if requester_participant.joined_at and participant.joined_at:
                if requester_participant.joined_at > participant.joined_at:
                    raise ValidationError("A newer admin cannot remove an older admin.")

            # count admins by role (not by user_id) and prevent removing the last admin
            admin_count = ConversationParticipant.objects.filter(conversation=conversation, role=RoleEnum.ADMIN).count()
            if admin_count <= 1:
                raise ValidationError("Cannot remove the last admin from the conversation.")

        participant.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    # ------------------------
    # Messages endpoints
    # ------------------------
    @action(detail=True, methods=["get", "post"], url_path="messages", pagination_class=SetPagination)
    def messages(self, request, pk=None):
        """
        Combined messages endpoint:
         - GET: return paginated persisted messages plus pending Redis messages.
         - POST: queue a new message via Redis for fast delivery.
        """
        conversation = self.get_object()
        self._ensure_participant(conversation, request.user)

        if request.method.lower() == "get":
            db_qs = ConversationMessage.objects.filter(conversation=conversation).select_related("sender").order_by("created_at")
            paginator = SetPagination()
            page = paginator.paginate_queryset(db_qs, request)
            db_data = ConversationMessageSerializer(
                page,
                many=True,
                context={"request": request, "conversation": conversation},
            ).data

            pending_list = fetch_messages_from_kafka(conversation.id)
            count_of_persisted_data = len(db_data)
            pending_data_count = len(pending_list)
            for i in range(pending_data_count):
                pending_list[i]["id"] = count_of_persisted_data + i + 1
                del pending_list[i]["_kafka_topic"]
                del pending_list[i]["_kafka_partition"]
                del pending_list[i]["_kafka_offset"]
            return paginator.get_paginated_response({"messages": db_data + pending_list})

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
        }
        temp_id = uuid4()
        produce_message(conversation.id, payload)
        return Response({"status": "queued", "temp_id": temp_id, "payload": payload}, status=status.HTTP_202_ACCEPTED)

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
            # cast items to int when possible
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
        # Ensure requester belongs to the conversation before returning participants
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

    # helpers
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
        # Limit to receipts in conversations the user participates in
        return qs.filter(message__conversation__participants__user=self.request.user).distinct()

    def perform_create(self, serializer):
        message_id = self.request.data.get("message")
        if not message_id:
            raise ValidationError("message is required.")
        message = get_object_or_404(ConversationMessage, pk=message_id)
        if not ConversationParticipant.objects.filter(conversation_id=message.conversation_id, user=self.request.user).exists():
            raise PermissionDenied("You are not a participant in this conversation.")
        serializer.save(user=self.request.user)
