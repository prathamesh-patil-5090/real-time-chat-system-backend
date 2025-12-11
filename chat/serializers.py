from django.contrib.auth import get_user_model
from rest_framework import serializers
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.pagination import PageNumberPagination

from chat.models import (
    Conversation,
    ConversationMessage,
    ConversationParticipant,
    MessageReadReceipt,
)

User = get_user_model()


class SetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 1000


#
# Conversation message serializer
# - sender is read-only and set to request.user
# - conversation is read-only (expected to be provided by the view via serializer.context['conversation'])
# - validates that request.user is a participant of the conversation
# - validates reply_to belongs to the same conversation
#
class ConversationMessageSerializer(serializers.ModelSerializer):
    sender = serializers.PrimaryKeyRelatedField(read_only=True)
    conversation = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = ConversationMessage
        fields = [
            'id',
            'conversation',
            'sender',
            'content',
            'message_type',
            'reply_to',
            'is_deleted',
            'created_at',
            'updated_at'
        ]
        read_only_fields = [
            'id',
            'conversation',
            'sender',
            'created_at',
            'updated_at'
        ]

    def validate(self, attrs):
        """
        Validate reply_to (if present) belongs to the same conversation and that
        the request user is participant of the conversation.
        """
        request = self.context.get('request')
        conversation = self.context.get('conversation') or attrs.get('conversation')
        if conversation is None:
            raise ValidationError("Conversation must be provided (view should pass it in serializer context).")

        # ensure request user is participant
        user = getattr(request, 'user', None)
        if user is None or not user.is_authenticated:
            raise PermissionDenied("Authentication required to send messages.")

        is_participant = ConversationParticipant.objects.filter(
            conversation=conversation,
            user=user
        ).exists()
        if not is_participant:
            raise PermissionDenied("You are not a participant of this conversation.")

        # reply_to validation: same conversation
        reply_to = attrs.get('reply_to')
        if reply_to is not None and reply_to.conversation_id != conversation.id:
            raise ValidationError("reply_to must belong to the same conversation.")

        return attrs

    def create(self, validated_data):
        """
        Create message with sender=request.user and conversation from context.
        """
        request = self.context.get('request')
        conversation = self.context.get('conversation') or validated_data.pop('conversation', None)

        if conversation is None:
            raise ValidationError("Conversation must be provided in serializer context or data.")

        message = ConversationMessage.objects.create(
            conversation=conversation,
            sender=request.user,
            **validated_data
        )

        # Note: updating Conversation.updated_at or broadcasting should be handled by the view or signals
        return message


#
# Conversation participant serializer
# - conversation and user are read-only on updates/creates from clients (view should control them)
# - role changes are allowed only if the requester is an admin of the conversation
#
class ConversationParticipantsSerializer(serializers.ModelSerializer):
    conversation = serializers.PrimaryKeyRelatedField(read_only=True)
    user = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = ConversationParticipant
        fields = [
            'id',
            'conversation',
            'user',
            'role',
            'joined_at',
            'is_muted',
            'last_read_message'
        ]
        read_only_fields = [
            'id',
            'conversation',
            'user',
            'joined_at',
            'last_read_message'
        ]

    def validate(self, attrs):
        """
        No extra field-level validation here — critical checks happen in update/create.
        """
        return attrs

    def update(self, instance, validated_data):
        """
        Enforce role changes only by admins. Allow muting by the user themself or admins.
        """
        request = self.context.get('request')
        user = getattr(request, 'user', None)
        if user is None or not user.is_authenticated:
            raise PermissionDenied("Authentication required.")

        # If role is being changed, requester must be an admin in the conversation
        new_role = validated_data.get('role')
        if new_role and new_role != instance.role:
            # check requester is admin
            is_admin = ConversationParticipant.objects.filter(
                conversation=instance.conversation,
                user=user,
                role='ADMIN'
            ).exists()
            if not is_admin:
                raise PermissionDenied("Only conversation admins can change roles.")

        # If trying to change someone else's muted status, ensure permission:
        if 'is_muted' in validated_data and instance.user != user:
            # only admin may mute/unmute other users
            is_admin = ConversationParticipant.objects.filter(
                conversation=instance.conversation,
                user=user,
                role='ADMIN'
            ).exists()
            if not is_admin:
                raise PermissionDenied("Only conversation admins can mute/unmute other users.")

        # allow member to toggle their own is_muted
        for attr, val in validated_data.items():
            setattr(instance, attr, val)
        instance.save()
        return instance

    def create(self, validated_data):
        """
        For creation, view should supply 'conversation' and 'user' (or you can extend logic here).
        Keep creation responsibility in the view (so proper permissions are enforced).
        """
        # We still provide a safe fallback if view put conversation & user in context
        conversation = self.context.get('conversation') or validated_data.get('conversation')
        user = self.context.get('user') or validated_data.get('user')
        if not conversation or not user:
            raise ValidationError("Both conversation and user must be provided by the view/context when creating participants.")
        # Let the model/db enforce unique_together
        participant = ConversationParticipant.objects.create(
            conversation=conversation,
            user=user,
            role=validated_data.get('role', ConversationParticipant._meta.get_field('role').get_default()),
            is_muted=validated_data.get('is_muted', False)
        )
        return participant


#
# Message read receipt serializer
# - user is read-only and set to request.user
# - validate that the request.user is a participant of the message's conversation
#
class MessageReadReceiptsSerializer(serializers.ModelSerializer):
    user = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = MessageReadReceipt
        fields = [
            'id',
            'message',
            'user',
            'read_at'
        ]
        read_only_fields = [
            'id',
            'user',
            'read_at'
        ]

    def validate(self, attrs):
        """
        Ensure the message exists and request.user is participant of that message's conversation.
        """
        request = self.context.get('request')
        user = getattr(request, 'user', None)
        if user is None or not user.is_authenticated:
            raise PermissionDenied("Authentication required.")

        message = attrs.get('message')
        if message is None:
            raise ValidationError("message is required.")

        is_participant = ConversationParticipant.objects.filter(
            conversation_id=message.conversation_id,
            user=user
        ).exists()
        if not is_participant:
            raise PermissionDenied("You are not a participant in the message's conversation.")

        return attrs

    def create(self, validated_data):
        """
        Create a read receipt for request.user only.
        """
        request = self.context.get('request')
        user = getattr(request, 'user', None)
        if user is None or not user.is_authenticated:
            raise PermissionDenied("Authentication required.")

        message = validated_data.get('message')
        # avoid IntegrityError by using get_or_create so duplicate receipts return the existing one
        read_receipt, created = MessageReadReceipt.objects.get_or_create(
            message=message,
            user=user
        )
        return read_receipt


#
# Conversation serializer
# - created_by is read-only and automatically set from request in create()
#
class ConversationSerializer(serializers.ModelSerializer):
    created_by = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Conversation
        fields = [
            'id',
            'is_group',
            'name',
            'created_by',
            'created_at',
            'updated_at'
        ]
        read_only_fields = [
            'id',
            'created_by',
            'created_at',
            'updated_at'
        ]

    def create(self, validated_data):
        """
        Ensure created_by is set to request.user (do not trust client input).
        """
        request = self.context.get('request')
        if not request or not getattr(request, 'user', None) or not request.user.is_authenticated:
            raise PermissionDenied("Authentication required.")

        validated_data['created_by'] = request.user
        return super().create(validated_data)

    def update(self, instance, validated_data):
        """
        Prevent changing created_by; allow other fields.
        """
        validated_data.pop('created_by', None)
        for attr, val in validated_data.items():
            setattr(instance, attr, val)
        instance.save()
        return instance
