from authentication.serializers import UserSerializer
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
    sender = UserSerializer(read_only=True)
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

        user = getattr(request, 'user', None)
        if user is None or not user.is_authenticated:
            raise PermissionDenied("Authentication required to send messages.")

        is_participant = ConversationParticipant.objects.filter(
            conversation=conversation,
            user=user
        ).exists()
        if not is_participant:
            raise PermissionDenied("You are not a participant of this conversation.")

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

        return message


#
# Conversation participant serializer
# - conversation and user are read-only on updates/creates from clients (view should control them)
# - role changes are allowed only if the requester is an admin of the conversation
#
class ConversationParticipantsSerializer(serializers.ModelSerializer):
    conversation = serializers.PrimaryKeyRelatedField(read_only=True)
    user = UserSerializer(read_only=True)

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

        new_role = validated_data.get('role')
        if new_role and new_role != instance.role:
            is_admin = ConversationParticipant.objects.filter(
                conversation=instance.conversation,
                user=user,
                role='ADMIN'
            ).exists()
            if not is_admin:
                raise PermissionDenied("Only conversation admins can change roles.")

        if 'is_muted' in validated_data and instance.user != user:
            is_admin = ConversationParticipant.objects.filter(
                conversation=instance.conversation,
                user=user,
                role='ADMIN'
            ).exists()
            if not is_admin:
                raise PermissionDenied("Only conversation admins can mute/unmute other users.")

        for attr, val in validated_data.items():
            setattr(instance, attr, val)
        instance.save()
        return instance

    def create(self, validated_data):
        """
        For creation, view should supply 'conversation' and 'user' (or you can extend logic here).
        Keep creation responsibility in the view (so proper permissions are enforced).
        """
        conversation = self.context.get('conversation') or validated_data.get('conversation')
        user = self.context.get('user') or validated_data.get('user')
        if not conversation or not user:
            raise ValidationError("Both conversation and user must be provided by the view/context when creating participants.")
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
    user = UserSerializer(read_only=True)

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
        read_receipt, created = MessageReadReceipt.objects.get_or_create(
            message=message,
            user=user
        )
        return read_receipt


#
# Conversation serializer
# - created_by is read-only and automatically set from request in create()
# - includes latest_message, display_name, other_participant, and unread_count
#
class ConversationSerializer(serializers.ModelSerializer):
    created_by = serializers.PrimaryKeyRelatedField(read_only=True)
    latest_message = serializers.SerializerMethodField()
    display_name = serializers.SerializerMethodField()
    other_participant = serializers.SerializerMethodField()
    unread_count = serializers.SerializerMethodField()

    class Meta:
        model = Conversation
        fields = [
            'id',
            'is_group',
            'name',
            'display_name',
            'other_participant',
            'created_by',
            'latest_message',
            'unread_count',
            'created_at',
            'updated_at'
        ]
        read_only_fields = [
            'id',
            'created_by',
            'created_at',
            'updated_at',
            'display_name',
            'other_participant',
            'latest_message',
            'unread_count'
        ]

    def get_latest_message(self, obj):
        """
        Return the latest message from DB.
        Note: Kafka messages should be handled in the view layer.
        """
        latest = obj.messages.order_by('-created_at').first()
        if latest:
            return {
                'id': latest.id,
                'content': latest.content,
                'message_type': latest.message_type,
                'sender_id': latest.sender_id,
                'sender_name': f"{latest.sender.first_name} {latest.sender.last_name}".strip() or latest.sender.username,
                'created_at': latest.created_at,
                'is_deleted': latest.is_deleted
            }
        return None

    def get_display_name(self, obj):
        """
        For one-on-one conversations, return the other participant's name.
        For group conversations, return the conversation name.
        """
        if obj.is_group:
            return obj.name or f"Group {obj.id}"
        
        request = self.context.get('request')
        if request and request.user:
            other_participant = obj.participants.exclude(user=request.user).select_related('user').first()
            if other_participant:
                user = other_participant.user
                return f"{user.first_name} {user.last_name}".strip() or user.username
        
        return obj.name or f"Conversation {obj.id}"

    def get_other_participant(self, obj):
        """
        For one-on-one conversations, return the other participant's info.
        For group conversations, return None.
        """
        if obj.is_group:
            return None
        
        request = self.context.get('request')
        if request and request.user:
            other_participant = obj.participants.exclude(user=request.user).select_related('user').first()
            if other_participant:
                user = other_participant.user
                return {
                    'id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'email': user.email
                }
        
        return None

    def get_unread_count(self, obj):
        """
        Count unread messages for the current user.
        A message is unread if it's after the user's last_read_message.
        """
        request = self.context.get('request')
        if not request or not request.user:
            return 0
        
        participant = obj.participants.filter(user=request.user).first()
        if not participant:
            return 0
        
        if participant.last_read_message:
            unread = obj.messages.filter(
                created_at__gt=participant.last_read_message.created_at
            ).exclude(sender=request.user).count()
            return unread
        else:
            return obj.messages.exclude(sender=request.user).count()

    def validate(self, attrs):
        """
        Validate conversation data:
        - For personal chats (is_group=False), name is optional and will be ignored
        - For group chats (is_group=True), name is required
        """
        is_group = attrs.get('is_group', False)
        name = attrs.get('name')
        
        if is_group and not name:
            raise ValidationError({"name": "Group conversations must have a name."})
        
        if not is_group:
            attrs['name'] = None
        
        return attrs

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
        For personal chats, ignore name updates.
        """
        validated_data.pop('created_by', None)
        
        if not instance.is_group:
            validated_data.pop('name', None)
        
        for attr, val in validated_data.items():
            setattr(instance, attr, val)
        instance.save()
        return instance
