
import logging
from urllib.parse import parse_qs

from channels.db import database_sync_to_async
from channels.middleware import BaseMiddleware
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from rest_framework_simplejwt.exceptions import TokenError
from rest_framework_simplejwt.tokens import AccessToken

logger = logging.getLogger(__name__)
User = get_user_model()


@database_sync_to_async
def get_user_from_token(token_string):
    """
    Validate JWT token and return the user.
    """
    try:
        # Validate the token
        access_token = AccessToken(token_string)
        user_id = access_token['user_id']

        # Get the user
        user = User.objects.get(id=user_id)
        return user
    except (TokenError, User.DoesNotExist, KeyError) as e:
        logger.debug(f"Token validation failed: {e}")
        return AnonymousUser()


class JWTAuthMiddleware(BaseMiddleware):
    """
    Custom middleware that authenticates WebSocket connections using JWT tokens.

    Supports multiple token sources (in priority order):
    1. Query string: ?token=<jwt_access_token>
    2. Cookie: access_token (same as REST API)
    """
    async def __call__(self, scope, receive, send):
        # Try to get token from multiple sources
        token = None

        # 1. Try query string first (useful for testing with Postman)
        query_string = scope.get('query_string', b'').decode()
        query_params = parse_qs(query_string)
        token = query_params.get('token', [None])[0]

        # 2. If no query token, try cookies (same as REST API)
        if not token:
            headers = dict(scope.get('headers', []))
            cookie_header = headers.get(b'cookie', b'').decode()

            # Parse cookies manually
            cookies = {}
            for cookie in cookie_header.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    cookies[key] = value

            token = cookies.get('access_token')

        if token:
            # Validate token and get user
            scope['user'] = await get_user_from_token(token)
            logger.debug(f"WebSocket auth: user={scope['user']}, authenticated={getattr(scope['user'], 'is_authenticated', False)}")
        else:
            # No token provided
            scope['user'] = AnonymousUser()
            logger.debug("WebSocket auth: no token provided")

        return await super().__call__(scope, receive, send)
