from django.contrib.auth import authenticate, get_user_model
from rest_framework import serializers
from rest_framework_simplejwt.serializers import (
    TokenObtainPairSerializer,
    ValidationError,
)
from rest_framework_simplejwt.tokens import Token

from authentication.models import User

AuthUser = get_user_model()

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = [
            "id",
            "first_name",
            "last_name",
            "email",
            "username",
        ]

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(min_length=8, write_only=True)
    class Meta:
        model = User
        fields = [
            "id",
            "first_name",
            "last_name",
            "email",
            "username",
            "password",
            "created_at"
        ]

        read_only_fields = ["id", "created_at"]

    def create(self, validated_data):
        password = validated_data.pop('password')
        user = User.objects.create(**validated_data)
        user.set_password(password)
        user.save()
        return user

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    username_or_email = serializers.CharField(help_text="Enter your username or email")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if 'username' in self.fields:
            del self.fields['username']

    def validate(self, attrs):
        username_or_email = attrs.get('username_or_email')
        password = attrs.get('password')

        if not username_or_email or not password:
            raise ValidationError("Both the fields are required")

        if '@' in username_or_email:
            try:
                user = User.objects.get(email=username_or_email)
                username = user.username
            except User.DoesNotExist:
                raise ValidationError("Invalid email or password")
        else:
            username = username_or_email

        user = authenticate(username=username, password=password)

        if not user:
            raise ValidationError("Invalid credentials")
        if not user.is_active:
            raise ValidationError("User account is deactivated")

        attrs['username'] = username
        attrs.pop('username_or_email')
        data = super().validate(attrs)
        data['user'] = UserSerializer(user).data
        return data

    @classmethod
    def get_token(cls, user: AuthUser) -> Token:
        token = super().get_token(user)
        token['username'] = user.username
        token['email'] = user.email
        return token
