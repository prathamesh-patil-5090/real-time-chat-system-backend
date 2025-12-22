from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register(r'conversations', views.ConversationViewSet, basename='conversation')
router.register(r'participants', views.ConversationParticipantViewSet, basename='conversation-participant')
router.register(r'read-receipts', views.MessageReadReceiptViewSet, basename='read-receipt')

urlpatterns = [
    path('api/', include(router.urls)),
]
