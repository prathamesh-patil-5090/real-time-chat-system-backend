from django.conf import settings
from django.db import models
from django.db.models import Index, UniqueConstraint
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


class RoleEnum(models.TextChoices):
    MEMBER = 'MEMBER', _('Member')
    ADMIN = 'ADMIN', _('Admin')


class MessageType(models.TextChoices):
    TEXT = 'TEXT', _('Text')
    IMAGE = 'IMAGE', _('Image')
    FILE = 'FILE', _("File")


class Conversation(models.Model):
    is_group = models.BooleanField(default=False)
    name = models.CharField(null=True, max_length=255, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="created_conversations"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']
        indexes = [
            Index(fields=['updated_at']),
        ]

    def __str__(self):
        return self.name or f"Conversation {self.id}"


class ConversationMessage(models.Model):
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name="messages"
    )
    sender = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="sent_messages"
    )
    # allow content to be null/blank for non-text messages (e.g. IMAGE/FILE)
    content = models.TextField(blank=True, null=True)
    message_type = models.CharField(
        choices=MessageType.choices,
        default=MessageType.TEXT,
        max_length=10
    )
    reply_to = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        related_name="replies",
        null=True,
        blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_deleted = models.BooleanField(default=False)

    class Meta:
        ordering = ['created_at']
        indexes = [
            Index(fields=['conversation', 'created_at']),
        ]

    def __str__(self):
        sender = getattr(self.sender, "pk", None)
        return f"Message {self.pk} by {sender} in Conversation {self.conversation_id}"

    def save(self, *args, **kwargs):
        """
        When a new message is created, update the conversation's updated_at
        so conversation ordering reflects the most recent activity.

        We update the conversation timestamp with a QuerySet.update to avoid
        triggering extra save() recursion or signals here.
        """
        is_new = self._state.adding
        super().save(*args, **kwargs)
        if is_new and self.conversation_id:
            # set to now() explicitly so updated_at moves forward
            Conversation.objects.filter(pk=self.conversation_id).update(updated_at=timezone.now())


class ConversationParticipant(models.Model):
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name="participants"
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="conversation_participants"
    )
    role = models.CharField(
        choices=RoleEnum.choices,
        default=RoleEnum.MEMBER,
        max_length=10
    )
    joined_at = models.DateTimeField(auto_now_add=True)
    is_muted = models.BooleanField(default=False)
    last_read_message = models.ForeignKey(
        ConversationMessage,
        on_delete=models.SET_NULL,
        related_name="read_by_participants",
        null=True,
        blank=True
    )

    class Meta:
        constraints = [
            UniqueConstraint(fields=['conversation', 'user'], name='unique_participant_per_conversation'),
        ]
        indexes = [
            Index(fields=['conversation']),
            Index(fields=['user']),
        ]

    def __str__(self):
        return f"Participant {getattr(self.user, 'pk', None)} in Conversation {self.conversation_id}"


class MessageReadReceiptManager(models.Manager):
    def get_or_create_receipt(self, *, message, user):
        """
        Convenience wrapper that ensures a single receipt per (message, user).
        Returns a tuple (instance, created) like get_or_create.
        """
        return self.get_or_create(message=message, user=user)


class MessageReadReceipt(models.Model):
    message = models.ForeignKey(
        ConversationMessage,
        on_delete=models.CASCADE,
        related_name="read_receipts"
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="message_read_receipts"
    )
    read_at = models.DateTimeField(auto_now_add=True)

    objects = MessageReadReceiptManager()

    class Meta:
        constraints = [
            UniqueConstraint(fields=['message', 'user'], name='unique_read_receipt_per_user_message'),
        ]
        indexes = [
            Index(fields=['message']),
            Index(fields=['user']),
        ]

    def __str__(self):
        return f"ReadReceipt message={self.message_id} user={getattr(self.user, 'pk', None)}"
