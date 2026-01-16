from django.urls import include, path
from rest_framework.routers import DefaultRouter

from authentication import views

router = DefaultRouter()

router.register(r"login", views.LoginViewSet, basename="login")
router.register(r"register", views.RegisterViewSet, basename="register")
router.register(r"logout", views.LogoutViewSet, basename="logout")
router.register(r"profile", views.ProfileViewSet, basename="profile")
router.register(r"search", views.SearchProfileViewSet, basename="search")
router.register(r"user", views.ProfileHolderViewSet, basename="profile-holder")
router.register(r"users", views.AllUsersViewSet, basename="all-users")
router.register(r"refresh", views.RefreshViewSet, basename="refresh")

urlpatterns = [
    path("", include(router.urls))
]
