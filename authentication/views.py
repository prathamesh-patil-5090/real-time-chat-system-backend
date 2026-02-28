from numbers import Number
from ssl import get_server_certificate

from django.conf import settings
from django.db.models import Q
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.generics import get_object_or_404
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from rest_framework.validators import ValidationError
from rest_framework_simplejwt.exceptions import TokenError
from rest_framework_simplejwt.serializers import TokenRefreshSerializer
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenBlacklistView

from authentication.serializers import (
    CustomTokenObtainPairSerializer,
    RegisterSerializer,
    UserSerializer,
)

from .models import User

access_max_age = 86400
refresh_max_age = 604800

def set_jwt_cookies(response, access_token=True, refresh_token=True):
    secure_cookie = getattr(settings, "SESSION_COOKIE_SECURE", False)
    if not access_token or not refresh_token:
        raise ValidationError("Need both the tokens")
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=secure_cookie,
        max_age=access_max_age
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=secure_cookie,
        max_age=refresh_max_age
    )

class LoginViewSet(viewsets.ModelViewSet):
    queryset = None
    serializer_class = CustomTokenObtainPairSerializer
    permission_classes = [AllowAny]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            access_token = serializer.validated_data.get('access')
            refresh_token = serializer.validated_data.get('refresh')
            response = Response( {
                "message" : 'User logged in successfully',
            },status=HTTP_200_OK)
            set_jwt_cookies(response, access_token, refresh_token)
            return response
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)

class RegisterViewSet(viewsets.ModelViewSet):
    queryset = None
    serializer_class = RegisterSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            refresh = RefreshToken.for_user(user)
            access_token = str(refresh.access_token)
            refresh_token = str(refresh)
            response = Response({
                "message" : 'User signed up successfully',
                "user" : UserSerializer(user).data
            }, status=HTTP_201_CREATED)
            set_jwt_cookies(response, access_token, refresh_token)
            return response
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)

class LogoutViewSet(viewsets.ModelViewSet):
    queryset = None
    serializer_class = UserSerializer
    permission_classes = [AllowAny]

    def create(self, request):
        refresh_token = request.data.get("refresh") or request.COOKIES.get("refresh_token")

        if not refresh_token:
            return Response(
                {"detail": "Refresh token is required."},
                status=HTTP_400_BAD_REQUEST
            )
        try:
            token = RefreshToken(refresh_token)
            token.blacklist()
            response = Response({
                "message": "User logged successfully"
            }, status=HTTP_200_OK)
            response.delete_cookie("access_token")
            response.delete_cookie("refresh_token")
            return response
        except TokenError as e:
            return Response({
                "error": str(e)
            }, status = HTTP_400_BAD_REQUEST)

class RefreshViewSet(viewsets.ModelViewSet):
    serializer_class = TokenRefreshSerializer
    queryset = User.objects.none()
    permission_classes = [AllowAny]

    def create(self, request):
        refresh_token = request.COOKIES.get("refresh_token")

        if not refresh_token:
            return Response(
                {"detail": "Refresh token is required."},
                status=HTTP_400_BAD_REQUEST
            )

        try:
            serializer = self.get_serializer({"refresh" : refresh_token})
            if serializer.is_valid():
                new_refresh_token = serializer.validated_data['refresh']
                access_token = serializer.validated_data['access']
                response = Response({
                    "message": "User access token refreshed successfully"
                }, status=HTTP_200_OK)
                set_jwt_cookies(response, access_token, refresh_token=new_refresh_token)
                return response
            return Response({
                "error": serializer.errors
            }, status=HTTP_400_BAD_REQUEST)

        except TokenError as e:
            return Response({
                "error": str(e)
            }, status=HTTP_400_BAD_REQUEST)

class ProfileViewSet(viewsets.ModelViewSet):
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    queryset = User.objects.all()

    def list(self, request, *args, **kwargs):
        profile = self.get_serializer(request.user).data
        return Response({
            "message": "Profile fetched successfully",
            "user": profile
        }, status=HTTP_200_OK)

class ProfileHolderViewSet(viewsets.ModelViewSet):
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    queryset = User.objects.all()

    def list(self, request, *args, **kwargs):
        request_id = request.quest_params.get("requestId")
        if not request_id:
            return Response(
                        {"detail": "requestId query parameter is required."},
                        status=HTTP_400_BAD_REQUEST
                    )
        user = get_object_or_404(User, pk=request_id)
        return Response({
            "message": "Holder Profile fetched successfully",
            "user": self.get_serializer(user).data
        }, status=HTTP_200_OK)


class SearchProfileViewSet(viewsets.ModelViewSet):
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    queryset = User.objects.all()  

    def list(self, request, *args, **kwargs):
        search_params = request.query_params.get("search_params")

        if not search_params:
            return Response(
                {"detail": "search_params query parameter is required."},
                status=HTTP_400_BAD_REQUEST
            )

        search_params = search_params.strip()

        query = Q(username__icontains=search_params) | \
                Q(first_name__icontains=search_params) | \
                Q(last_name__icontains=search_params) | \
                Q(email__icontains=search_params)

        if search_params.isdigit():
            query |= Q(id=int(search_params))

        users = User.objects.filter(query).exclude(id=request.user.id).distinct()

        if not users.exists():
            return Response(
                {"detail": "No users found matching the search criteria."},
                status=HTTP_400_BAD_REQUEST
            )

        return Response({
            "message": "Users fetched successfully",
            "users": self.get_serializer(users, many=True).data,
            "count": users.count()
        }, status=HTTP_200_OK)


class AllUsersViewSet(viewsets.ModelViewSet):
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    queryset = User.objects.all()

    def list(self, request, *args, **kwargs):
        """
        Return all users on the platform with pagination support.
        """
        users = User.objects.all().order_by('id')

        return Response({
            "message": "All users fetched successfully",
            "users": self.get_serializer(users, many=True).data,
            "count": users.count()
        }, status=HTTP_200_OK)
