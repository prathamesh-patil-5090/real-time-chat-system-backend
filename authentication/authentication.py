from rest_framework_simplejwt.authentication import JWTAuthentication


class CookieJWTAuthentication(JWTAuthentication):
    def authenticate(self, request):
        cookie_token = request.COOKIES.get('access_token')
        if cookie_token:
            validated_token = self.get_validated_token(cookie_token)
            return self.get_user(validated_token), validated_token
        return super().authenticate(request)
