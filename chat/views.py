import json
from typing import AsyncGenerator  # kept only if you later add streaming APIs

from django.contrib.auth import get_user_model
from django.http import HttpRequest, StreamingHttpResponse  # HttpRequest for type hints
from django.shortcuts import get_object_or_404
from rest_framework import permissions, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response

from authentication.authentication import CookieJWTAuthentication
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

class ConversationViewSet(viewsets.ModelViewSet):
    serializer_class = ConversationSerializer
    authentication_classes = [CookieJWTAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return (
            Conversation.objects.filter(participants__user=user)
            .select_related('created_by')
            .distinct()
        )

    def perform_create(self, serializer):
        conversation = serializer.save()
        ConversationParticipant.objects.get_or_create(
            conversation=conversation,
            user=self.request.user,
            defaults={'role': RoleEnum.ADMIN}
        )
        extra_participant_ids = self._normalize_id_list(self.request.data.get('participant_ids',[]))
        for user_id in extra_participant_ids:
            if str(user_id) == str(self.request.user.id):
                continue
                ConversationParticipant.objects.get_or_create(
                    conversation=conversation,
                    user_id=user_id,
                    defaults={'role':RoleEnum.MEMBER}
                )

    def perform_update(self, serializer):
        conversation = self.get_object()
        if not self._is_admin(conversation, self.request.user):
            raise PermissionDenied("Only conversation admins can update conversation settings.")
            serializer.save()

    def destroy(self, request, *args, **kwargs):
        conversation = self.get_object()
        if not self._is_admin(conversation, self.request.user):
            raise PermissionDenied("Only conversation admins can delete a conversation.")
        return super().destroy(request, *args, **kwargs)

    @action(detail=True, methods=['get'], url_path='participants')
    def list_participants(self, request, pk=None):
        conversation = self.get_object()
        participants = ConversationParticipant.objects.filter(conversation=conversation).select_related('user')
        data = ConversationParticipantsSerializer(participants, many=True).data
        return Response(data)

    @action(detail=True, methods=['post'], url_path='participants')
    def add_participants(self, request, pk=None):
        conversation = self.get_object()
        if not self._is_admin(conversation, self.request.user):
            raise PermissionDenied("Only conversation admins can add a participant to a conversation.")
        user_id = self.request.get('user_id')
        if not user_id:
            raise ValidationError("User Id is required")
        participant, _ = ConversationParticipant.objects.get_or_create(
            conversation=conversation,
            user_id=user_id,
            default={'role':RoleEnum.MEMBER}
        )
        data=ConversationParticipantsSerializer(participant).data
        return Response(data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=['delete'], url_path='participants')
    def remove_participants(self, request, pk=None):
        conversation = self.get_object()
        if not self._is_admin(conversation, self.request.user):
            raise PermissionDenied("Only conversation admins can delete a participant from a conversation.")
            user_id = self.request.get('user_id')
            if not user_id:
                raise ValidationError("User Id is required")
        participant = get_object_or_404(
            ConversationParticipant,
            conversation=conversation,
            user_id=user_id,
        )
        if participant.role == RoleEnum.ADMIN:
            requester_participantt
            admin_count = ConversationParticipant.objects.filter(
                conversation=conversation,
                user_id=user_id
            ).count()
            if admin_count <= 1:
                raise ValidationError("Cannot remove the last admin from the conversation.")
        participant.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
