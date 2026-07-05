"""Bearer token authentication."""

import hmac
from typing import NoReturn

from fastapi import Header, HTTPException, status

from pgrelay.config.settings import Settings


def check_bearer_token(authorization: str, tokens: set[str]) -> bool:
    """Check bearer token without raising on non-ASCII input"""
    authorization_bytes = authorization.encode()
    for token in tokens:
        expected = f"Bearer {token}"
        expected_bytes = expected.encode()
        if hmac.compare_digest(authorization_bytes, expected_bytes):
            return True
    return False


def raise_unauthorized(message: str) -> NoReturn:
    """Raise a common unauthorized API error."""
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "error": {
                "code": "unauthorized",
                "message": message,
                "details": {},
            }
        },
    )


def raise_forbidden(message: str) -> NoReturn:
    """Raise a common forbidden API error."""
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={
            "error": {
                "code": "forbidden",
                "message": message,
                "details": {},
            }
        },
    )


async def require_api_token(
    settings: Settings,
    authorization: str | None = Header(default=None),
) -> None:
    """Require a configured bearer token when API tokens are configured."""
    tokens = settings.get_api_tokens() | settings.get_read_only_api_tokens()
    if not tokens:
        return
    if authorization is None:
        raise_unauthorized("Missing bearer token")

    if not check_bearer_token(authorization, tokens):
        raise_unauthorized("Invalid bearer token")


async def require_api_write_token(
    settings: Settings,
    authorization: str | None = Header(default=None),
) -> None:
    """Require a bearer token with write access."""
    read_tokens = settings.get_api_tokens() | settings.get_read_only_api_tokens()
    write_tokens = settings.get_api_tokens()
    if not read_tokens:
        return
    if authorization is None:
        raise_unauthorized("Missing bearer token")
    if check_bearer_token(authorization, write_tokens):
        return
    if check_bearer_token(authorization, read_tokens):
        raise_forbidden("Bearer token is read-only")
    raise_unauthorized("Invalid bearer token")
